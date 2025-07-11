-- Create watermark table for incremental loads
CREATE TABLE watermarktable (
    TableName VARCHAR(255) PRIMARY KEY,
    WatermarkValue DATETIME2,
    CreatedDate DATETIME2 DEFAULT GETDATE(),
    ModifiedDate DATETIME2 DEFAULT GETDATE()
);

-- Insert initial watermark values
INSERT INTO watermarktable (TableName, WatermarkValue)
VALUES ('SourceTable', '1900-01-01 00:00:00.000');

-- Create stored procedure to update watermark
CREATE PROCEDURE usp_write_watermark 
    @LastModifiedtime DATETIME2,
    @TableName VARCHAR(255)
AS
BEGIN
    UPDATE watermarktable
    SET WatermarkValue = @LastModifiedtime,
        ModifiedDate = GETDATE()
    WHERE TableName = @TableName;
END;