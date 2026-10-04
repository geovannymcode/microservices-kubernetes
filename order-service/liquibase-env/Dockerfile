# Liquibase 5 images ship without JDBC drivers. Same CLI version as the app (Boot 4.1.1 manages 5.0.3)
# and the same PostgreSQL driver (42.7.13), downloaded from Maven Central and verified by checksum.
FROM liquibase/liquibase:5.0.3@sha256:dfe136c26d39088d87d1bd27f833d6ac5b738a180d7c960b065c7be525725d01
ADD --checksum=sha256:6e0e4cc2d8cae902084f8a2b18728b073a6fd9d1f87c9d8bff8f298c18185b93 --chmod=644 \
    https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.13/postgresql-42.7.13.jar /liquibase/lib/
