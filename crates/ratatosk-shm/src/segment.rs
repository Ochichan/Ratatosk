//! Anonymous shared-memory segment: creation, mapping, and atomic views.
//!
//! This is the only module (besides `fdpass`) that contains `unsafe`. All
//! accesses to the mapping go through `AtomicU64` / `AtomicU32` / `AtomicU8`
//! views; the crate never creates a `&[u8]` or `&mut [u8]` over the mapping,
//! because another process (possibly hostile) can mutate it at any time.

use std::{
    io,
    os::fd::{AsFd, AsRawFd, BorrowedFd, FromRawFd, OwnedFd},
    ptr::NonNull,
    sync::atomic::{AtomicU8, AtomicU32, AtomicU64, Ordering},
};

use crate::{
    layout::{self, LayoutError},
    ring::RingView,
};

/// A mapped shared-memory segment holding both ring directions.
pub struct Segment {
    base: NonNull<u8>,
    len: usize,
    fd: OwnedFd,
    /// Private copy: never re-read from the (peer-writable) header.
    ring_bytes: u32,
}

impl std::fmt::Debug for Segment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Segment")
            .field("len", &self.len)
            .field("ring_bytes", &self.ring_bytes)
            .field("fd", &self.fd.as_raw_fd())
            .finish()
    }
}

// SAFETY: the mapping is only ever accessed through atomics, and the fd is an
// owned handle; both are safe to move to and share between threads.
unsafe impl Send for Segment {}
unsafe impl Sync for Segment {}

impl Segment {
    /// Create a fresh anonymous segment of the given per-direction ring size,
    /// zero-filled by the kernel, and write the header. Server side.
    pub fn create(ring_bytes: u32) -> io::Result<Self> {
        let ring_bytes = layout::validate_ring_bytes(ring_bytes).map_err(invalid)?;
        let len = layout::segment_bytes(ring_bytes);
        let fd = create_anonymous_fd(len)?;
        let segment = Self::map(fd, len, ring_bytes)?;
        segment
            .header_u64(layout::OFF_MAGIC)
            .store(layout::MAGIC, Ordering::Relaxed);
        segment
            .header_u32(layout::OFF_VERSION)
            .store(layout::VERSION, Ordering::Relaxed);
        segment
            .header_u32(layout::OFF_RING_BYTES)
            .store(ring_bytes, Ordering::Release);
        Ok(segment)
    }

    /// Map a segment received from the peer and validate its header. Client side.
    pub fn from_fd(fd: OwnedFd) -> io::Result<Self> {
        let len = file_len(&fd)?;
        let max = layout::segment_bytes(layout::MAX_RING_BYTES);
        if len < layout::HEADER_BYTES || len > max {
            return Err(invalid(LayoutError::SegmentTooSmall {
                expected: layout::HEADER_BYTES,
                actual: len,
            }));
        }
        // Map with a provisional ring size, then validate from the header.
        let mut provisional = Self::map(fd, len, layout::MIN_RING_BYTES)?;
        let magic = provisional
            .header_u64(layout::OFF_MAGIC)
            .load(Ordering::Acquire);
        if magic != layout::MAGIC {
            return Err(invalid(LayoutError::BadMagic(magic)));
        }
        let version = provisional
            .header_u32(layout::OFF_VERSION)
            .load(Ordering::Acquire);
        if version != layout::VERSION {
            return Err(invalid(LayoutError::BadVersion(version)));
        }
        let ring_bytes = provisional
            .header_u32(layout::OFF_RING_BYTES)
            .load(Ordering::Acquire);
        let ring_bytes = layout::validate_ring_bytes(ring_bytes).map_err(invalid)?;
        let expected = layout::segment_bytes(ring_bytes);
        if len < expected {
            return Err(invalid(LayoutError::SegmentTooSmall {
                expected,
                actual: len,
            }));
        }
        provisional.ring_bytes = ring_bytes;
        Ok(provisional)
    }

    fn map(fd: OwnedFd, len: usize, ring_bytes: u32) -> io::Result<Self> {
        // SAFETY: mapping a file descriptor we own with PROT_READ|PROT_WRITE and
        // MAP_SHARED; `len` is bounded by the caller and checked against the
        // file length. The pointer is validated against MAP_FAILED below.
        let ptr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                len,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                fd.as_raw_fd(),
                0,
            )
        };
        if ptr == libc::MAP_FAILED {
            return Err(io::Error::last_os_error());
        }
        let base = NonNull::new(ptr.cast::<u8>())
            .ok_or_else(|| io::Error::other("mmap returned a null mapping"))?;
        Ok(Self {
            base,
            len,
            fd,
            ring_bytes,
        })
    }

    pub fn fd(&self) -> BorrowedFd<'_> {
        self.fd.as_fd()
    }

    pub fn ring_bytes(&self) -> u32 {
        self.ring_bytes
    }

    fn header_u64(&self, off: usize) -> &AtomicU64 {
        debug_assert!(off + 8 <= layout::HEADER_BYTES && off % 8 == 0);
        // SAFETY: `off` is a compile-time layout constant inside the header,
        // 8-byte aligned, and the mapping is page aligned and at least
        // HEADER_BYTES long. The shared word is only accessed atomically.
        unsafe { AtomicU64::from_ptr(self.base.as_ptr().add(off).cast::<u64>()) }
    }

    fn header_u32(&self, off: usize) -> &AtomicU32 {
        debug_assert!(off + 4 <= layout::HEADER_BYTES && off % 4 == 0);
        // SAFETY: as for `header_u64`, with 4-byte alignment.
        unsafe { AtomicU32::from_ptr(self.base.as_ptr().add(off).cast::<u32>()) }
    }

    fn data(&self, off: usize) -> &[AtomicU8] {
        let len = self.ring_bytes as usize;
        debug_assert!(off + len <= self.len);
        // SAFETY: the range [off, off+len) lies inside the mapping (checked by
        // the constructor against the file length); `AtomicU8` has the same
        // layout as `u8`, and every access goes through atomic loads/stores so
        // concurrent writes by the peer process cannot cause a data race in the
        // Rust abstract machine.
        unsafe { std::slice::from_raw_parts(self.base.as_ptr().add(off).cast::<AtomicU8>(), len) }
    }

    /// Client-to-server ring: the client produces, the server consumes.
    pub fn c2s(&self) -> RingView<'_> {
        RingView {
            tail: self.header_u64(layout::OFF_C2S_TAIL),
            head: self.header_u64(layout::OFF_C2S_HEAD),
            consumer_parked: self.header_u32(layout::OFF_C2S_CONSUMER_PARKED),
            producer_parked: self.header_u32(layout::OFF_C2S_PRODUCER_PARKED),
            data: self.data(layout::off_c2s_data()),
        }
    }

    /// Server-to-client ring: the server produces, the client consumes.
    pub fn s2c(&self) -> RingView<'_> {
        RingView {
            tail: self.header_u64(layout::OFF_S2C_TAIL),
            head: self.header_u64(layout::OFF_S2C_HEAD),
            consumer_parked: self.header_u32(layout::OFF_S2C_CONSUMER_PARKED),
            producer_parked: self.header_u32(layout::OFF_S2C_PRODUCER_PARKED),
            data: self.data(layout::off_s2c_data(self.ring_bytes)),
        }
    }
}

impl Drop for Segment {
    fn drop(&mut self) {
        // SAFETY: `base`/`len` came from a successful mmap in `map` and are
        // unmapped exactly once here. The fd is closed by `OwnedFd`.
        unsafe {
            libc::munmap(self.base.as_ptr().cast(), self.len);
        }
    }
}

fn invalid(error: LayoutError) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error)
}

fn file_len(fd: &OwnedFd) -> io::Result<usize> {
    // SAFETY: `stat` is a plain out-parameter struct; fstat only writes into it.
    let mut stat: libc::stat = unsafe { std::mem::zeroed() };
    // SAFETY: valid fd and a valid pointer to a `stat` buffer.
    if unsafe { libc::fstat(fd.as_raw_fd(), &mut stat) } != 0 {
        return Err(io::Error::last_os_error());
    }
    usize::try_from(stat.st_size)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "negative segment length"))
}

#[cfg(target_os = "linux")]
fn create_anonymous_fd(len: usize) -> io::Result<OwnedFd> {
    let name = c"ratatosk-shm";
    // SAFETY: valid NUL-terminated name; flags are plain constants.
    let raw =
        unsafe { libc::memfd_create(name.as_ptr(), libc::MFD_CLOEXEC | libc::MFD_ALLOW_SEALING) };
    if raw < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: `raw` is a freshly created, exclusively owned descriptor.
    let fd = unsafe { OwnedFd::from_raw_fd(raw) };
    let size = libc::off_t::try_from(len)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "segment too large"))?;
    // SAFETY: valid fd; ftruncate only changes the file length.
    if unsafe { libc::ftruncate(fd.as_raw_fd(), size) } != 0 {
        return Err(io::Error::last_os_error());
    }
    // Freeze the size so neither side can shrink the mapping under the other.
    // SAFETY: valid fd and constant flags.
    if unsafe {
        libc::fcntl(
            fd.as_raw_fd(),
            libc::F_ADD_SEALS,
            libc::F_SEAL_SHRINK | libc::F_SEAL_GROW | libc::F_SEAL_SEAL,
        )
    } != 0
    {
        return Err(io::Error::last_os_error());
    }
    Ok(fd)
}

#[cfg(not(target_os = "linux"))]
fn create_anonymous_fd(len: usize) -> io::Result<OwnedFd> {
    use std::sync::atomic::AtomicU32 as Counter;
    static COUNTER: Counter = Counter::new(0);

    // POSIX shm names are limited (macOS PSHMNAMLEN = 31 including the slash).
    let name = format!(
        "/rtk{:x}-{:x}",
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::Relaxed)
    );
    let cname = std::ffi::CString::new(name)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "shm name"))?;
    // SAFETY: valid NUL-terminated name; O_EXCL guarantees we never open a
    // pre-existing object created by someone else.
    let raw = unsafe {
        libc::shm_open(
            cname.as_ptr(),
            libc::O_RDWR | libc::O_CREAT | libc::O_EXCL,
            0o600,
        )
    };
    if raw < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: freshly created, exclusively owned descriptor.
    let fd = unsafe { OwnedFd::from_raw_fd(raw) };
    // macOS shm_open rejects O_CLOEXEC (EINVAL), so set the flag afterwards.
    // SAFETY: valid fd; F_SETFD only changes the close-on-exec flag.
    if unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_SETFD, libc::FD_CLOEXEC) } != 0 {
        return Err(io::Error::last_os_error());
    }
    // Remove the name immediately: from here on the object lives only as long
    // as descriptors/mappings exist, so a crash leaves nothing behind.
    // SAFETY: valid NUL-terminated name.
    let unlinked = unsafe { libc::shm_unlink(cname.as_ptr()) };
    if unlinked != 0 {
        return Err(io::Error::last_os_error());
    }
    let size = libc::off_t::try_from(len)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "segment too large"))?;
    // macOS allows exactly one ftruncate on a POSIX shm object; this is it.
    // SAFETY: valid fd.
    if unsafe { libc::ftruncate(fd.as_raw_fd(), size) } != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(fd)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_and_reopen_round_trip() {
        let segment = Segment::create(layout::MIN_RING_BYTES).expect("create");
        assert_eq!(segment.ring_bytes(), layout::MIN_RING_BYTES);
        let dup = segment.fd().try_clone_to_owned().expect("dup fd");
        let reopened = Segment::from_fd(dup).expect("reopen");
        assert_eq!(reopened.ring_bytes(), layout::MIN_RING_BYTES);

        // The two mappings alias the same memory.
        segment.c2s().tail.store(42, Ordering::SeqCst);
        assert_eq!(reopened.c2s().tail.load(Ordering::SeqCst), 42);
        segment.c2s().data[7].store(9, Ordering::SeqCst);
        assert_eq!(reopened.c2s().data[7].load(Ordering::SeqCst), 9);
    }

    #[test]
    fn corrupted_header_is_rejected_on_reopen() {
        let segment = Segment::create(layout::MIN_RING_BYTES).expect("create");
        segment
            .header_u32(layout::OFF_RING_BYTES)
            .store(layout::MAX_RING_BYTES, Ordering::SeqCst);
        let dup = segment.fd().try_clone_to_owned().expect("dup fd");
        let error = Segment::from_fd(dup).expect_err("size mismatch must fail");
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);

        segment
            .header_u64(layout::OFF_MAGIC)
            .store(0, Ordering::SeqCst);
        let dup = segment.fd().try_clone_to_owned().expect("dup fd");
        let error = Segment::from_fd(dup).expect_err("bad magic must fail");
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn rejects_invalid_ring_size() {
        assert!(Segment::create(4095).is_err());
    }
}
