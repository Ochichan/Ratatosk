# Ratatosk 자동실행 정리 (systemd --user)

이 문서는 "재부팅 후 Ratatosk 자동실행" 설정에서 필요한 핵심만 한 번에 보려고 만든 정리본입니다.

## 1) 지금 상태 해석

`systemctl --user status ratatosk` 결과가 아래처럼 나오면:

- `ratatosk.service`
- `Loaded: masked`
- `Active: inactive (dead)`

이건 **오류가 아니라 정상일 수 있음**.

이 프로젝트는 레거시 이름 `ratatosk.service`를 막고(mask), 실제 서비스는 `ratatosk-serve.service`를 쓰도록 되어 있습니다.

근거 파일:
- `scripts/install-ratatosk-autostart.sh`
- `docs/ecosystem.md`

## 2) 실제로 확인할 서비스 이름

항상 아래 서비스 기준으로 확인:

```bash
ratatosk-serve.service
```

## 3) 설치/활성화 명령 (한 번 실행)

fish 사용자 권장(쉘 문법 차이 회피):

```bash
cd /home/ochi/Documents/Project/Ratatosk
./scripts/recover-ratatosk-autostart.sh
```

직접 설치만 하고 싶으면:

```bash
cd /home/ochi/Documents/Project/Ratatosk
./scripts/install-ratatosk-autostart.sh
```

이 스크립트가 하는 일:
- 런처 설치: `~/.local/bin/ratatosk`
- 유닛 설치: `~/.config/systemd/user/ratatosk-serve.service`
- 유닛 enable + start
- 필요 시 legacy `ratatosk.service` mask
- 가능하면 `loginctl enable-linger` 시도

## 4) 상태/로그 확인

```bash
systemctl --user daemon-reload
systemctl --user enable --now ratatosk-serve.service
systemctl --user status ratatosk-serve.service --no-pager -l
journalctl --user -u ratatosk-serve.service -n 100 --no-pager
```

## 5) 재부팅 직후(로그인 전) 자동실행까지 필요할 때

```bash
sudo loginctl enable-linger "$USER"
loginctl show-user "$USER" -p Linger
```

`Linger=yes`면 로그인하지 않아도 user service가 부팅 후 올라옵니다.

## 6) 포트 주의사항

autostart 기본 포트는 `6380`입니다.
- 이유: 수동 실행 기본 포트 `6379`와 충돌 방지

fish/Bash 공통으로 포트를 바꾸려면 스크립트 사용:

```bash
cd /home/ochi/Documents/Project/Ratatosk
./scripts/set-ratatosk-autostart-port.sh 6379
```

## 7) 빠른 진단 체크리스트

1. 유닛 파일 존재 확인
```bash
ls -l ~/.config/systemd/user/ratatosk-serve.service
```

2. enable 여부
```bash
systemctl --user is-enabled ratatosk-serve.service
```

3. active 여부
```bash
systemctl --user is-active ratatosk-serve.service
```

4. 프로세스 포트 리슨 확인
```bash
ss -ltnp | rg '6379|6380|ratatosk'
```

5. 클라이언트 ping 확인(포트에 맞춰서)
```bash
redis-cli -p 6380 ping
```

## 8) 자주 막히는 지점

- `insufficient open-file limit: soft_limit=1024 required_at_least=4224`:
  기본 `RATATOSK_MAX_CLIENTS=4096` 대비 nofile가 낮아서 발생.
  최신 스크립트는 런처에서 nofile를 읽어 `RATATOSK_MAX_CLIENTS`를 자동 보정하고,
  유닛에 `LimitNOFILE=65535`를 설정한다.
  변경 반영 후 아래 순서로 복구:
  ```bash
  cd /home/ochi/Documents/Project/Ratatosk
  ./scripts/install-ratatosk-autostart.sh
  systemctl --user reset-failed ratatosk-serve.service
  systemctl --user restart ratatosk-serve.service
  systemctl --user status ratatosk-serve.service --no-pager -l
  ```

- `Failed to connect to user scope bus`:
  user systemd 세션/권한 문제. 실제 로그인 셸에서 다시 시도.

- `fish에서 명령이 안 먹음`:
  heredoc 등 Bash 문법 차이. 수동 입력 대신 아래 스크립트 사용:
  - `./scripts/recover-ratatosk-autostart.sh`
  - `./scripts/set-ratatosk-autostart-port.sh 6379`

- `Permission denied` on `~/.config/systemd/user`:
  현재 셸 권한/환경 문제. 본인 계정 홈 디렉토리 권한 점검.

- 서비스가 계속 재시작:
  `journalctl --user -u ratatosk-serve.service -f`로 즉시 에러 원인 확인.

## 9) 리셋 후 재설치(필요 시)

```bash
systemctl --user disable --now ratatosk-serve.service
rm -f ~/.config/systemd/user/ratatosk-serve.service
systemctl --user daemon-reload
./scripts/install-ratatosk-autostart.sh
```
