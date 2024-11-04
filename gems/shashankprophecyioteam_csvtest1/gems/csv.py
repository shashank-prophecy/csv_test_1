from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType

from prophecy.cb.server.base.ComponentBuilderBase import ComponentCode, Diagnostic, SeverityLevelEnum
from prophecy.cb.server.base.DatasetBuilderBase import DatasetSpec, DatasetProperties, Component
from prophecy.cb.server.base.datatypes import SString, SFloat
from prophecy.cb.ui.uispec import *
from prophecy.cb.util.NumberUtils import parseFloat
from prophecy.cb.server.base import WorkflowContext
import dataclasses
from prophecy.cb.migration import PropertyMigrationObj


class csv(DatasetSpec):
    name: str = "AdhocCSV"
    datasetType: str = "File"
    docUrl: str = "https://docs.prophecy.io/low-code-spark/gems/source-target/file/csv"

    def optimizeCode(self) -> bool:
        return True

    @dataclass(frozen=True)
    class CsvProperties(DatasetProperties):
        schema: Optional[StructType] = None
        description: Optional[str] = ""
        useSchema: Optional[bool] = True
        path: str = ""
        separator: Optional[SString] = SString(",")
        encoding: Optional[str] = None
        quote: Optional[str] = None
        escape: Optional[str] = None
        charToEscapeQuoteEscaping: Optional[str] = None
        header: Optional[bool] = True
        ignoreLeadingWhiteSpaceReading: Optional[bool] = None
        ignoreTrailingWhiteSpaceReading: Optional[bool] = None
        ignoreLeadingWhiteSpaceWriting: Optional[bool] = None
        ignoreTrailingWhiteSpaceWriting: Optional[bool] = None
        nullValue: Optional[str] = None
        emptyValue: Optional[str] = None
        dateFormat: Optional[str] = None
        timestampFormat: Optional[str] = None
        comment: Optional[str] = None
        enforceSchema: Optional[bool] = None
        inferSchema: Optional[bool] = None
        samplingRatio: Optional[SFloat] = None
        nanValue: Optional[str] = None
        positiveInf: Optional[str] = None
        negativeInf: Optional[str] = None
        maxColumns: Optional[str] = None
        maxCharsPerColumn: Optional[str] = None
        unescapedQuoteHandling: Optional[str] = None
        mode: Optional[str] = None
        columnNameOfCorruptRecord: Optional[str] = None
        multiLine: Optional[bool] = None
        escapeQuotes: Optional[bool] = None
        quoteAll: Optional[bool] = None
        compression: Optional[str] = None
        partitionColumns: Optional[List[str]] = None
        writeMode: Optional[str] = "error"
        locale: Optional[str] = None
        lineSep: Optional[str] = None
        pathGlobFilter: Optional[str] = None
        modifiedBefore: Optional[str] = None
        modifiedAfter: Optional[str] = None
        recursiveFileLookup: Optional[bool] = None
        createSingleOutputFile: Optional[bool] = None
        pathSelection: Optional[str] = "fileLocation"
        targetPathSelection: Optional[str] = "fileLocation"
        optionalCopyDestination: Optional[str] = None
        secretUsername: SecretValue = field(default_factory=list)
        secretPassword: SecretValue = field(default_factory=list)
        secretUrl: SecretValue = field(default_factory=list)
        sftpOptionalCopyDestination: Optional[str] = None
        sftpSecretUsername: SecretValue = field(default_factory=list)
        sftpSecretPassword: SecretValue = field(default_factory=list)
        sftpSecretUrl: SecretValue = field(default_factory=list)
        sftpSourcePath: Optional[str] = None

    def sourceDialog(self) -> DatasetDialog:
        sharepointDialog = (StackLayout(direction=("vertical"), gap=("1rem"))
                            .addElement(
            AlertBox(
                variant="info",
                _children=[
                    Markdown(
                        "Double check the [Office365-REST-Python-Client](https://pypi.org/project/Office365-REST-Python-Client/) (Python Package) PyPi dependency is configured for this pipeline and is installed on the Spark cluster\n"
                    )
                ]
            )
        )
                            .addElement(
            StackLayout()
                .addElement(
                ColumnsLayout(gap="1rem")
                    .addColumn(SecretBox("Username").bindPlaceholder("username").bindProperty("secretUsername"))
                    .addColumn(
                    SecretBox("Password").isPassword().bindPlaceholder("password").bindProperty("secretPassword"))
            )
        )
                            .addElement(TitleElement(title="URL"))
                            .addElement(
            SecretBox("Sharepoint Relative URL")
                .bindPlaceholder("https://xxx.sharepoint.com/personal/xxx0_onmicrosoft_com")
                .bindProperty("secretUrl")
        )
                            .addElement(TextBox("Sharepoint Location")
                                        .bindPlaceholder("/personal/xxx/Documents/customers.csv")
                                        .bindProperty("path")
                                        )
                            .addElement(TextBox("Destination (Data will be copied here from sharepoint)")
                                        .bindPlaceholder("dbfs:/sharepoint/customers.csv")
                                        .bindProperty("optionalCopyDestination")
                                        )
                            )

        sftpDialog = (StackLayout(direction=("vertical"), gap=("1rem"))
                      .addElement(
            AlertBox(
                variant="info",
                _children=[
                    Markdown(
                        "Double check the [paramiko](https://pypi.org/project/paramiko/) (Python Package) PyPi dependency is configured for this pipeline and is installed on the Spark cluster\n"
                    )
                ]
            )
        )
                      .addElement(
            StackLayout()
                .addElement(
                ColumnsLayout(gap="1rem")
                    .addColumn(SecretBox("Username").bindPlaceholder("username").bindProperty("sftpSecretUsername"))
                    .addColumn(
                    SecretBox("Password").isPassword().bindPlaceholder("password").bindProperty("sftpSecretPassword"))
            )
        )
                      .addElement(TitleElement(title="Source SFTP Details"))
                      .addElement(
            SecretBox("Host")
                .bindPlaceholder("104.197.67.53")
                .bindProperty("sftpSecretUrl")
        )
                      .addElement(TextBox("Source File Path On SFTP")
                                  .bindPlaceholder("/home/sftpuser/customers.csv")
                                  .bindProperty("sftpSourcePath")
                                  )
                      .addElement(TextBox("Destination (Data will be copied here from sftp)")
                                  .bindPlaceholder("dbfs:/sftp/customers.csv")
                                  .bindProperty("sftpOptionalCopyDestination")
                                  )
                      )

        locationSelection = (RadioGroup("Read from:")
                             .addOption("File Location", "fileLocation")
                             .addOption("Sharepoint", "sharepoint")
                             .addOption("SFTP", "sftp")
                             .bindProperty("pathSelection"))

        return DatasetDialog("csv") \
            .addSection("LOCATION", StackLayout().addElement(locationSelection)
                        .addElement(
            Condition()
                .ifEqual(PropExpr("component.properties.pathSelection"), StringExpr("fileLocation"))
                .then(TargetLocation("path"))
        )
                        .addElement(
            Condition()
                .ifEqual(PropExpr("component.properties.pathSelection"), StringExpr("sharepoint"))
                .then(sharepointDialog)
        )
                        .addElement(
            Condition()
                .ifEqual(PropExpr("component.properties.pathSelection"), StringExpr("sftp"))
                .then(sftpDialog)
        )
                        ) \
            .addSection(
            "PROPERTIES",
            ColumnsLayout(gap="1rem", height="100%")
                .addColumn(
                ScrollBox()
                    .addElement(
                    StackLayout(height="100%")
                        .addElement(
                        StackItem(grow=1).addElement(
                            FieldPicker(height="100%")
                                .addField(
                                TextArea("Description", 2,
                                         placeholder="Dataset description...").withCopilotEnabledDescribeDataSource(),
                                "description",
                                True
                            )
                                .addField(Checkbox("Use user-defined schema"), "useSchema", True)
                                .addField(TextBox("Column delimiter").bindPlaceholder("").enableEscapeSequence(),
                                          "separator")
                                .addField(Checkbox("First row is header"), "header")
                                .addField(Checkbox("Infer schema from data"), "inferSchema")
                                .addField(Checkbox("Parse Multi-line records"), "multiLine")
                                .addField(TextBox("Encoding Type").bindPlaceholder(""), "encoding")
                                .addField(TextBox("Quote character").bindPlaceholder(""), "quote")
                                .addField(TextBox("Escape character").bindPlaceholder(""), "escape")
                                .addField(
                                TextBox("Escape char for quote escaping char").bindPlaceholder(""),
                                "charToEscapeQuoteEscaping"
                            )
                                .addField(TextBox("Skip line beginning with character").bindPlaceholder(""), "comment")
                                .addField(Checkbox("Enforce specified or inferred schema"), "enforceSchema")
                                .addField(TextBox("Sampling Ratio").bindPlaceholder(""), "samplingRatio")
                                .addField(Checkbox("Ignore leading white spaces from values"),
                                          "ignoreLeadingWhiteSpaceReading")
                                .addField(Checkbox("Ignore trailing white spaces from values"),
                                          "ignoreTrailingWhiteSpaceReading")
                                .addField(TextBox("Null Value").bindPlaceholder(""), "nullValue")
                                .addField(TextBox("Empty Value").bindPlaceholder(""), "emptyValue")
                                .addField(TextBox("String representation for non-number value").bindPlaceholder(""),
                                          "nanValue")
                                .addField(TextBox("Positive infinity value").bindPlaceholder(""), "positiveInf")
                                .addField(TextBox("Negative infinity value").bindPlaceholder(""), "negativeInf")
                                .addField(TextBox("Date format string").bindPlaceholder(""), "dateFormat")
                                .addField(TextBox("Timestamp format string").bindPlaceholder(""), "timestampFormat")
                                .addField(TextBox("Max number of columns per record").bindPlaceholder(""), "maxColumns")
                                .addField(
                                TextBox("Allowed maximum characters per column").bindPlaceholder(""),
                                "maxCharsPerColumn"
                            )
                                .addField(
                                SelectBox("Corrupt record handling")
                                    .addOption("PERMISSIVE", "permissive")
                                    .addOption("DROPMALFORMED", "dropmalformed")
                                    .addOption("FAILFAST", "failfast"),
                                "mode"
                            )
                                .addField(
                                TextBox("Column name of a corrupt record").bindPlaceholder(""),
                                "columnNameOfCorruptRecord"
                            )
                                .addField(TextBox("Line Sep").bindPlaceholder(""), "lineSep")
                                .addField(TextBox("Locale").bindPlaceholder(""), "locale")
                                .addField(
                                SelectBox("Unescaped Quote Handling")
                                    .addOption("STOP_AT_CLOSING_QUOTE", "STOP_AT_CLOSING_QUOTE")
                                    .addOption("BACK_TO_DELIMITER", "BACK_TO_DELIMITER")
                                    .addOption("STOP_AT_DELIMITER", "STOP_AT_DELIMITER")
                                    .addOption("SKIP_VALUE", "SKIP_VALUE")
                                    .addOption("RAISE_ERROR", "RAISE_ERROR"),
                                "unescapedQuoteHandling"
                            )
                                .addField(Checkbox("Recursive File Lookup"), "recursiveFileLookup")
                                .addField(TextBox("Path Global Filter").bindPlaceholder(""), "pathGlobFilter")
                                .addField(TextBox("Modified Before").bindPlaceholder(""), "modifiedBefore")
                                .addField(TextBox("Modified After").bindPlaceholder(""), "modifiedAfter")
                        )
                    )
                ),
                "auto"
            )
                .addColumn(SchemaTable("").bindProperty("schema"), "5fr")
        ) \
            .addSection(
            "PREVIEW",
            PreviewTable("").bindProperty("schema")
        )

    def targetDialog(self) -> DatasetDialog:
        locationSelection = (RadioGroup("Write to:")
                             .addOption("File Location", "fileLocation")
                             .addOption("Sharepoint (Coming Soon)", "sharepoint")
                             .addOption("SFTP (Coming Soon)", "sftp")
                             .bindProperty("targetPathSelection"))

        return DatasetDialog("csv") \
            .addSection("LOCATION", StackLayout().addElement(locationSelection)
                        .addElement(
            Condition()
                .ifEqual(PropExpr("component.properties.targetPathSelection"), StringExpr("fileLocation"))
                .then(TargetLocation("path"))
        )
                        ) \
            .addSection(
            "PROPERTIES",
            ColumnsLayout(gap="1rem", height="100%")
                .addColumn(
                ScrollBox().addElement(
                    StackLayout(height="100%").addElement(
                        StackItem(grow=1).addElement(
                            FieldPicker(height="100%")
                                .addField(
                                TextArea("Description", 2,
                                         placeholder="Dataset description...").withCopilotEnabledDescribeDataSource(),
                                "description",
                                True
                            )
                                .addField(
                                SelectBox("Write Mode")
                                    .addOption("error", "error")
                                    .addOption("overwrite", "overwrite")
                                    .addOption("append", "append")
                                    .addOption("ignore", "ignore"),
                                "writeMode"
                            )
                                .addField(
                                SchemaColumnsDropdown("Partition Columns")
                                    .withMultipleSelection()
                                    .bindSchema("schema")
                                    .bindProperty("partitionColumns"),
                                "partitionColumns"
                            )
                                .addField(TextBox("Column delimiter").bindPlaceholder("").enableEscapeSequence(),
                                          "separator")
                                .addField(Checkbox("First row is header"), "header")
                                .addField(TextBox("Encoding Type").bindPlaceholder(""), "encoding")
                                .addField(TextBox("Quote character").bindPlaceholder(""), "quote")
                                .addField(TextBox("Escape character").bindPlaceholder(""), "escape")
                                .addField(
                                TextBox("Escape char for quote escaping char").bindPlaceholder(""),
                                "charToEscapeQuoteEscaping"
                            )
                                .addField(TextBox("Null Value").bindPlaceholder(""), "nullValue")
                                .addField(TextBox("Empty Value").bindPlaceholder(""), "emptyValue")
                                .addField(
                                SelectBox("Compression")
                                    .addOption("none", "none")
                                    .addOption("bzip2", "bzip2")
                                    .addOption("gzip", "gzip")
                                    .addOption("lz4", "lz4")
                                    .addOption("snappy", "snappy")
                                    .addOption("deflate", "deflate"),
                                "compression"
                            )
                                .addField(Checkbox("Escape quotes"), "escapeQuotes")
                                .addField(Checkbox("Quote All"), "quoteAll")
                                .addField(TextBox("Date format string").bindPlaceholder(""), "dateFormat")
                                .addField(TextBox("Timestamp format string").bindPlaceholder(""), "timestampFormat")
                                .addField(Checkbox("Ignore leading white spaces from values"),
                                          "ignoreLeadingWhiteSpaceWriting")
                                .addField(Checkbox("Ignore trailing white spaces from values"),
                                          "ignoreTrailingWhiteSpaceWriting")
                                .addField(TextBox("Line Sep").bindPlaceholder(""), "lineSep")
                                .addField(Checkbox("Create single CSV file"), "createSingleOutputFile")

                        )
                    )
                ),
                "auto"
            )
                .addColumn(SchemaTable("").isReadOnly().withoutInferSchema().bindProperty("schema"), "5fr")
        )

    def validate(self, context: WorkflowContext, component: Component) -> list:
        diagnostics = super(csv, self).validate(context, component)
        if component.properties.separator.diagnosticMessages is not None:
            for message in component.properties.separator.diagnosticMessages:
                diagnostics.append(
                    Diagnostic("properties.separator", message, SeverityLevelEnum.Error))

        if component.properties.targetPathSelection in ["sharepoint", "sftp"]:
            diagnostics.append(
                Diagnostic("properties.targetPathSelection",
                           f"Write to {component.properties.targetPathSelection} coming soon !!",
                           SeverityLevelEnum.Error))

        if component.properties.targetPathSelection not in ["sharepoint", "sftp"] and (
                component.properties.pathSelection == "sharepoint" or component.properties.pathSelection == "fileLocation") and len(
                component.properties.path) == 0:
            diagnostics.append(
                Diagnostic("properties.path", "path variable cannot be empty [Location]", SeverityLevelEnum.Error))

        if component.properties.pathSelection == "sharepoint":
            if not component.properties.secretUsername.parts:
                diagnostics.append(Diagnostic("properties.secretUsername", "Username cannot be empty [Location]",
                                              SeverityLevelEnum.Error))
            if not component.properties.secretPassword.parts:
                diagnostics.append(Diagnostic("properties.secretPassword", "Password cannot be empty [Location]",
                                              SeverityLevelEnum.Error))

            if not component.properties.secretUrl.parts:
                diagnostics.append(
                    Diagnostic("properties.secretUrl", f"Sharepoint URL cannot be empty [Location]",
                               SeverityLevelEnum.Error))

        if component.properties.pathSelection == "sftp":
            if not component.properties.sftpSecretUsername.parts:
                diagnostics.append(Diagnostic("properties.sftpSecretUsername", "Username cannot be empty [Location]",
                                              SeverityLevelEnum.Error))
            if not component.properties.sftpSecretPassword.parts:
                diagnostics.append(Diagnostic("properties.sftpSecretPassword", "Password cannot be empty [Location]",
                                              SeverityLevelEnum.Error))

            if not component.properties.sftpSecretUrl.parts:
                diagnostics.append(
                    Diagnostic("properties.sftpSecretUrl", f"Host cannot be empty [Location]",
                               SeverityLevelEnum.Error))

            if not component.properties.sftpSourcePath:
                diagnostics.append(
                    Diagnostic("properties.sftpSourcePath", f"Source path cannot be empty [Location]",
                               SeverityLevelEnum.Error))

        if component.properties.samplingRatio is not None:
            if component.properties.samplingRatio.diagnosticMessages is not None:
                for message in component.properties.samplingRatio.diagnosticMessages:
                    diagnostics.append(
                        Diagnostic("properties.samplingRatio", message, SeverityLevelEnum.Error))

            floatValue = component.properties.samplingRatio.value
            if floatValue is not None and 0.0 < floatValue <= 1.0:
                return diagnostics
            else:
                diagnostics.append(
                    Diagnostic("properties.samplingRatio", "Sampling Ratio has to be between (0.0, 1.0] [Properties]",
                               SeverityLevelEnum.Error))
        if component.properties.columnNameOfCorruptRecord is not None:
            if component.properties.schema is None:
                diagnostics.append(Diagnostic(
                    "properties.schema",
                    "ColumnNameOfCorruptRecord will not work without a user-specified schema",
                    SeverityLevelEnum.Error
                ))
            if not component.properties.useSchema:
                diagnostics.append(Diagnostic(
                    "properties.columnNameOfCorruptRecord",
                    "ColumnNameOfCorruptRecord will only work if 'Use Schema' is enabled.",
                    SeverityLevelEnum.Error
                ))
        if component.properties.partitionColumns is not None and component.properties.createSingleOutputFile == True:
            diagnostics.append(Diagnostic(
                "properties.createSingleOutputFile",
                "Creation of single output file and partition columns cannot be used together.",
                SeverityLevelEnum.Error
            ))
        return diagnostics

    def onChange(self, context: WorkflowContext, oldState: Component, newState: Component) -> Component:
        if newState.properties.schema is not None:
            newSchema = newState.properties.schema
            schema = None
            (oldCR, newCR) = (
                oldState.properties.columnNameOfCorruptRecord, newState.properties.columnNameOfCorruptRecord)
            if oldCR is not None and newCR is None:
                schema = StructType(list(filter(lambda f: f.name != oldCR, newSchema)))
            elif oldCR is None and newCR is not None:
                if newCR not in newSchema.fieldNames():
                    schema = newSchema.add(StructField(newCR, StringType(), True))
                else:
                    schema = newSchema
            elif oldCR is not None and newCR is not None:
                without_old = StructType(list(filter(lambda f: f.name != oldCR, newSchema)))
                if newCR not in without_old.fieldNames():
                    schema = without_old.add(StructField(newCR, StringType(), True))
                else:
                    schema = without_old
            else:
                schema = newSchema
            return newState.bindProperties(dataclasses.replace(newState.properties, schema=schema))
        return newState

    class CsvFormatCode(ComponentCode):
        def __init__(self, props):
            self.props: csv.CsvProperties = props

        def sourceApply(self, spark: SparkSession) -> DataFrame:

            finalPath = self.props.path

            if self.props.pathSelection == "sharepoint":
                import os
                from office365.sharepoint.client_context import ClientContext
                from office365.runtime.auth.user_credential import UserCredential

                sharepointUrl = self.props.secretUrl
                ctx = ClientContext(sharepointUrl).with_credentials(
                    UserCredential(self.props.secretUsername, self.props.secretPassword)
                )
                file_url = self.props.path
                download_path = os.path.join("/tmp", os.path.basename(file_url))
                with open(download_path, "wb") as local_file:
                    ctx.web.get_file_by_server_relative_url(file_url).download(local_file).execute_query()

                finalPath = f"file://{download_path}"
                if self.props.optionalCopyDestination is not None and self.props.optionalCopyDestination != "":
                    from pyspark.dbutils import DBUtils
                    dbutils = DBUtils(spark)
                    dbutils.fs.cp(f"file://{download_path}", self.props.optionalCopyDestination)
                    finalPath = self.props.optionalCopyDestination
            elif self.props.pathSelection == "sftp":
                import paramiko
                import os

                def download_file_from_sftp(username, password, host, remote_file_path, local_file_path):
                    ssh_client: SubstituteDisabled = paramiko.SSHClient()
                    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                    try:
                        ssh_client.connect(host, username=username, password=password)
                        sftp_client = ssh_client.open_sftp()
                        try:
                            sftp_client.get(remote_file_path, local_file_path)
                        finally:
                            sftp_client.close()
                    finally:
                        ssh_client.close()

                download_path = os.path.join("/tmp", os.path.basename(self.props.sftpSourcePath))

                download_file_from_sftp(username=self.props.sftpSecretUsername, password=self.props.sftpSecretPassword,
                                        host=self.props.sftpSecretUrl, remote_file_path=self.props.sftpSourcePath,
                                        local_file_path=download_path)
                finalPath = f"file://{download_path}"
                if self.props.sftpOptionalCopyDestination is not None and self.props.sftpOptionalCopyDestination != "":
                    from pyspark.dbutils import DBUtils
                    dbutils = DBUtils(spark)
                    dbutils.fs.cp(f"file://{download_path}", self.props.sftpOptionalCopyDestination)
                    print(
                        "[Ok] file has been uploaded to DBFS path: {0}".format(self.props.sftpOptionalCopyDestination))
                    finalPath = self.props.sftpOptionalCopyDestination

            reader = spark.read
            if self.props.schema is not None and self.props.useSchema:
                reader = reader.schema(self.props.schema)
            if self.props.negativeInf is not None:
                reader = reader.option("negativeInf", self.props.negativeInf)
            if self.props.maxCharsPerColumn is not None:
                reader = reader.option("maxCharsPerColumn", self.props.maxCharsPerColumn)

            if self.props.header is not None:
                reader = reader.option("header", self.props.header)
            if self.props.inferSchema is not None:
                reader = reader.option("inferSchema", self.props.inferSchema)
            if self.props.mode is not None:
                reader = reader.option("mode", self.props.mode)
            if self.props.dateFormat is not None:
                reader = reader.option("dateFormat", self.props.dateFormat)
            if self.props.samplingRatio is not None:
                reader = reader.option("samplingRatio", self.props.samplingRatio.value)
            if self.props.positiveInf is not None:
                reader = reader.option("positiveInf", self.props.positiveInf)
            if self.props.escape is not None:
                reader = reader.option("escape", self.props.escape)
            if self.props.emptyValue is not None:
                reader = reader.option("emptyValue", self.props.emptyValue)
            if self.props.timestampFormat is not None:
                reader = reader.option("timestampFormat", self.props.timestampFormat)
            if self.props.quote is not None:
                reader = reader.option("quote", self.props.quote)
            if self.props.separator is not None:
                reader = reader.option("sep", self.props.separator.value)
            if self.props.enforceSchema is not None:
                reader = reader.option("enforceSchema", self.props.enforceSchema)
            if self.props.encoding is not None:
                reader = reader.option("encoding", self.props.encoding)
            if self.props.comment is not None:
                reader = reader.option("comment", self.props.comment)
            if self.props.locale is not None:
                reader = reader.option("locale", self.props.locale)
            if self.props.lineSep is not None:
                reader = reader.option("lineSep", self.props.lineSep)
            if self.props.unescapedQuoteHandling is not None:
                reader = reader.option("unescapedQuoteHandling", self.props.unescapedQuoteHandling)
            if self.props.charToEscapeQuoteEscaping is not None:
                reader = reader.option("charToEscapeQuoteEscaping", self.props.charToEscapeQuoteEscaping)
            if self.props.nanValue is not None:
                reader = reader.option("nanValue", self.props.nanValue)
            if self.props.ignoreLeadingWhiteSpaceWriting is not None:
                reader = reader.option("ignoreLeadingWhiteSpace", self.props.ignoreLeadingWhiteSpaceWriting)
            if self.props.ignoreTrailingWhiteSpaceWriting is not None:
                reader = reader.option("ignoreTrailingWhiteSpace", self.props.ignoreTrailingWhiteSpaceWriting)
            if self.props.nullValue is not None:
                reader = reader.option("nullValue", self.props.nullValue)
            if self.props.maxColumns is not None:
                reader = reader.option("maxColumns", self.props.maxColumns)
            if self.props.multiLine is not None:
                reader = reader.option("multiLine", self.props.multiLine)
            if self.props.modifiedBefore is not None:
                reader = reader.option("modifiedBefore", self.props.modifiedBefore)
            if self.props.modifiedAfter is not None:
                reader = reader.option("modifiedAfter", self.props.modifiedAfter)
            if self.props.recursiveFileLookup is not None:
                reader = reader.option("recursiveFileLookup", self.props.recursiveFileLookup)
            if self.props.pathGlobFilter is not None:
                reader = reader.option("pathGlobFilter", self.props.pathGlobFilter)
            if self.props.columnNameOfCorruptRecord is not None:
                reader = reader.option("columnNameOfCorruptRecord", self.props.columnNameOfCorruptRecord)

            return reader.csv(finalPath)

        def targetApply(self, spark: SparkSession, in0: DataFrame):
            writer = in0.write
            if self.props.header is not None:
                writer = writer.option("header", self.props.header)
            if self.props.dateFormat is not None:
                writer = writer.option("dateFormat", self.props.dateFormat)
            if self.props.escape is not None:
                writer = writer.option("escape", self.props.escape)
            if self.props.emptyValue is not None:
                writer = writer.option("emptyValue", self.props.emptyValue)
            if self.props.timestampFormat is not None:
                writer = writer.option("timestampFormat", self.props.timestampFormat)
            if self.props.quote is not None:
                writer = writer.option("quote", self.props.quote)
            if self.props.separator is not None:
                writer = writer.option("sep", self.props.separator.value)
            if self.props.quoteAll is not None:
                writer = writer.option("quoteAll", self.props.quoteAll)
            if self.props.encoding is not None:
                writer = writer.option("encoding", self.props.encoding)
            if self.props.charToEscapeQuoteEscaping is not None:
                writer = writer.option("charToEscapeQuoteEscaping", self.props.charToEscapeQuoteEscaping)
            if self.props.escapeQuotes is not None:
                writer = writer.option("escapeQuotes", self.props.escapeQuotes)
            if self.props.ignoreLeadingWhiteSpaceWriting is not None:
                writer = writer.option("ignoreLeadingWhiteSpace", self.props.ignoreLeadingWhiteSpaceWriting)
            if self.props.ignoreTrailingWhiteSpaceWriting is not None:
                writer = writer.option("ignoreTrailingWhiteSpace", self.props.ignoreTrailingWhiteSpaceWriting)
            if self.props.nullValue is not None:
                writer = writer.option("nullValue", self.props.nullValue)
            if self.props.compression is not None:
                writer = writer.option("compression", self.props.compression)
            if self.props.lineSep is not None:
                writer = writer.option("lineSep", self.props.lineSep)
            if self.props.writeMode is not None:
                writer = writer.mode(self.props.writeMode)
            if self.props.partitionColumns is not None and len(self.props.partitionColumns) > 0:
                writer = writer.partitionBy(*self.props.partitionColumns)
            writer = writer.option("separator", self.props.separator.value).option("header", self.props.header)
            if self.props.createSingleOutputFile is not None:
                writer.csv(self.props.path + "_temp")
            else:
                writer.csv(self.props.path)
            if self.props.createSingleOutputFile is not None:
                if self.props.createSingleOutputFile:
                    from prophecy.utils.gems_utils import concatenateFiles
                    concatenateFiles(spark, ".csv", self.props.writeMode, self.props.path + "_temp", self.props.path,
                                     True, True)

    def __init__(self):
        super().__init__()
        self.registerPropertyEvolution(CSVPropertyMigration())


class CSVPropertyMigration(PropertyMigrationObj):

    def migrationNumber(self) -> int:
        return 1

    def up(self, old_properties: csv.CsvProperties) -> csv.CsvProperties:
        return dataclasses.replace(
            old_properties,
            pathSelection="fileLocation"
        )

    def down(self, new_properties: csv.CsvProperties) -> csv.CsvProperties:
        raise Exception("Downgrade is not implemented for this CSV version")