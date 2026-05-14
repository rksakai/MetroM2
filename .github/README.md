.github/
├── workflows/
│   ├── ci.yml                  # CI: lint + testes (todo PR)
│   ├── cd-staging.yml          # CD: deploy staging (push main)
│   ├── cd-production.yml       # CD: deploy prod (tag v*)
│   ├── security-scan.yml       # Scan de segurança semanal
│   └── infra-deploy.yml        # Deploy infraestrutura Bicep
├── CODEOWNERS
└── pull_request_template.md
