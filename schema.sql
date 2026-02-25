IF OBJECT_ID('dbo.alt_file_state', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.alt_file_state (
        id BIGINT IDENTITY(1,1) PRIMARY KEY,
        step_name NVARCHAR(50) NOT NULL,
        file_name NVARCHAR(260) NOT NULL,
        business_date CHAR(8) NOT NULL,
        full_path NVARCHAR(400) NOT NULL,
        size_bytes BIGINT NOT NULL,
        arrived_at_utc DATETIME2 NULL,
        moved_at_utc DATETIME2 NULL,
        status NVARCHAR(20) NOT NULL,
        updated_at_utc DATETIME2 NOT NULL,
        CONSTRAINT uq_alt_file UNIQUE(step_name, file_name, business_date)
    );
END;

IF OBJECT_ID('dbo.alt_alert_log', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.alt_alert_log (
        id BIGINT IDENTITY(1,1) PRIMARY KEY,
        step_name NVARCHAR(50) NOT NULL,
        file_name NVARCHAR(260) NOT NULL,
        business_date CHAR(8) NOT NULL,
        alert_type NVARCHAR(50) NOT NULL,
        created_at_utc DATETIME2 NOT NULL
    );
    CREATE INDEX ix_alt_alert_1 ON dbo.alt_alert_log(step_name, file_name, business_date, alert_type);
END;

IF OBJECT_ID('dbo.alt_heartbeat', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.alt_heartbeat (
        id BIGINT IDENTITY(1,1) PRIMARY KEY,
        note NVARCHAR(200) NOT NULL,
        created_at_utc DATETIME2 NOT NULL
    );
END;
