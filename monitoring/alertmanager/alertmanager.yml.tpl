global:
  resolve_timeout: 5m
  # __SLACK_WEBHOOK_URL__ is substituted by the alertmanager container entrypoint
  # from the SLACK_WEBHOOK_URL env (compose); falls back to a dead URL when unset
  # so the config stays valid and alerts remain visible in the Alertmanager UI.
  slack_api_url: "__SLACK_WEBHOOK_URL__"
route:
  group_by: [severity, table]
  receiver: slack
  routes:
    - match:
        severity: critical
      receiver: slack-critical
    - match:
        severity: high
      receiver: slack
receivers:
  - name: slack
    slack_configs:
      - channel: "#data-alerts"
        title: "{{ .GroupLabels.severity }}: {{ .CommonAnnotations.summary }}"
        text: "{{ .CommonAnnotations.description }}{{ .CommonAnnotations.summary }}"
  - name: slack-critical
    slack_configs:
      - channel: "#data-critical"
        title: "{{ .GroupLabels.severity }}: {{ .CommonAnnotations.summary }}"
        text: "{{ .CommonAnnotations.summary }} runbook: {{ .CommonAnnotations.runbook }}"
