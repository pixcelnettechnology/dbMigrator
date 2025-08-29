IF DB_ID('srcdb') IS NULL CREATE DATABASE srcdb;
GO
USE srcdb;
GO
IF OBJECT_ID('[dbo].[types_demo]') IS NOT NULL DROP TABLE [dbo].[types_demo];
CREATE TABLE [dbo].[types_demo](
  [id] BIGINT IDENTITY(1,1) PRIMARY KEY,
  [name] NVARCHAR(120) NOT NULL,
  [active] BIT NOT NULL DEFAULT 1,
  [amount] DECIMAL(38,9) NULL,
  [payload] VARBINARY(MAX) NULL,
  [created_at] DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
  [notes] NVARCHAR(MAX) NULL
);
INSERT INTO [dbo].[types_demo] ([name],[active],[amount],[payload],[notes])
VALUES (N'row_1',1,123.456789123, 0x01, NULL),
       (N'row_2',0,999999.999999999, 0x02, N'hello');