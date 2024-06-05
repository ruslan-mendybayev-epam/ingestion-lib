USE TEST_DB
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

insert into [dbo].[DATA_TYPES_TABLE] (
    [binary_col],
    [varbinary_col],
    [char_col],
    [varchar_col],
    [nchar_col],
    [nvarchar_col],
    [datetime_col],
    [smalldatetime_col],
    [date_col],
    [time_col],
    [datetimeoffset_col],
    [datetime2_col],
    [decimal_col],
    [numeric_col],
    [float_col],
    [real_col],
    [bigint_col],
    [int_col],
    [smallint_col],
    [tinyint_col],
    [money_col],
    [smallmoney_col],
    [bit_col],
    [image_col],
    [ntext_col],
    [text_col],
    [xml_col],
    [uniqueidentifier_col],
    --[sql_variant_col], -- .SparkSQLException: [UNRECOGNIZED_SQL_TYPE] Unrecognized SQL type - name: sql_variant, id: -156.
	[geography_col],
	[geometry_col],
    [custom_varchar20_col],
    [hierarchyid_col]

) values (
0x15,
0x15,
'a',
'a',
'a',
'a',
'2020-12-01',
'2020-12-01',
'2020-12-01',
'10:34:23',
'12-10-25 12:32:10 +01:00',
'12-21-16',
476.29,
12345.12,
1245.12,
96.602,
15,
15,
15,
1,
3148.29,
3148.29,
1,
0xFFD8FFE000104A46494600010100004800480000FFED003B41646F62650064000000,
'testing 1,2,3',
'text',
'<employee><firstname type=\"textbox\">Jimmy</firstname></employee>',
NEWID(),
--CAST(46279.1 as decimal(8,2)),
geography::STGeomFromText('LINESTRING(-122.360 47.656, -122.343 47.656 )', 4326),
geometry::STGeomFromText('LINESTRING (100 100, 20 180, 180 180)', 0),
'custom varchar type',
hierarchyid::Parse('/1/')
)

GO
