# 마이그레이션 관측

공통 상태 API의 `details.object`가 live/acked/waiting/recovering 앱 수와 현재 generation을 제공합니다. expectedApps는 컨트롤 플레인 seed의 값이며 live가 줄어도 기대 수를 줄이지 않습니다. 앱 보고는 메모리 기반이라 재시작 후 재등록 전까지 미확인입니다.

initialCopyObservedAt/forwardObservedAt/reverseObservedAt은 실제 worker 보고 시각입니다. lastSuccessfulSyncAt/lastVerificationAt/lastProgressAt과 구분해서 제공합니다. API 조회 시각으로 복제의 최신성을 갱신하지 않습니다. 값이 있어도 측정 시각이 없다면 초기값일 수 있습니다.

Readiness 비교 항목의 verification은 SIZE_AND_MD5_WHEN_AVAILABLE, REGISTERED_BUCKETS와 matched/missing/different/extra를 제공합니다. MD5가 없는 객체는 크기 비교입니다. `target_has_source`의 missing은 이번 실행 중 보정 후 남은 수이고 matched는 최초 비교의 일치 수입니다. 역방향 복사 성공을 전체 내용 hash 검증으로 표시하지 않습니다.

컨트롤 플레인의 등록된 어댑터에서 5초마다 수집합니다. 검사: `go test ./...`.
