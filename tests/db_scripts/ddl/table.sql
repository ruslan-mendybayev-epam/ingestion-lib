USE TEST_DB
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TYPE custom_varchar20 FROM varchar(20) NOT NULL
GO

CREATE TABLE [dbo].[DATA_TYPES_TABLE](
	[sys_id] [int] IDENTITY(1,1) NOT FOR REPLICATION NOT NULL,
	[binary_col] [binary],
    [varbinary_col] [varbinary],
    [char_col] [char],
    [varchar_col] [varchar],
    [nchar_col] [nchar],
    [nvarchar_col] [nvarchar],
    [datetime_col] [datetime],
    [smalldatetime_col] [smalldatetime],
    [date_col] [date],
    [time_col] [time],
    [datetimeoffset_col] [datetimeoffset],
    [datetime2_col] [datetime2],
    [decimal_col] [decimal](5, 2),
    [numeric_col] [numeric](10, 2),
    [float_col] [float],
    [real_col] [real],
    [bigint_col] [bigint],
    [int_col] [int],
    [smallint_col] [smallint],
    [tinyint_col] [tinyint],
    [money_col] [money],
    [smallmoney_col] [smallmoney],
    [bit_col] [bit],
    [timestamp_col] [timestamp],
    [image_col] [image],
    [ntext_col] [ntext],
    [text_col] [text],
    [xml_col] [xml],
	[uniqueidentifier_col] [uniqueidentifier],
	--[sql_variant_col] [sql_variant] -- .SparkSQLException: [UNRECOGNIZED_SQL_TYPE] Unrecognized SQL type - name: sql_variant, id: -156.
	[geography_col] [geography],
	[geometry_col] [geometry],
	[custom_varchar20_col] [custom_varchar20],
	[hierarchyid_col] [hierarchyid]
)
