package io.github.antonkw

import Fibonacci.{fibonacci, validatedNode}
import org.apache.calcite.avatica.util.Casing.TO_UPPER
import org.apache.calcite.avatica.util.Quoting.DOUBLE_QUOTE
import org.apache.calcite.config.{CalciteConnectionConfig, CalciteConnectionProperty}
import org.apache.calcite.jdbc.{CalciteSchema, JavaTypeFactoryImpl}
import org.apache.calcite.plan.volcano.VolcanoPlanner
import org.apache.calcite.plan.{RelOptCluster, RelOptTable}
import org.apache.calcite.prepare.CalciteCatalogReader
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rex.RexBuilder
import org.apache.calcite.sql.{SqlNode, SqlWriterConfig}
import org.apache.calcite.sql.dialect.SnowflakeSqlDialect
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.calcite.sql.parser.{SqlAbstractParserImpl, SqlParser}
import org.apache.calcite.sql.pretty.SqlPrettyWriter
import org.apache.calcite.sql.util.SqlOperatorTables
import org.apache.calcite.sql.validate.{SqlValidator, SqlValidatorUtil}
import org.apache.calcite.sql2rel.{SqlToRelConverter, StandardConvertletTable}

import java.io.StringReader
import java.util.Collections




object Calcite {
  val parserConfig: SqlParser.Config =
    SqlParser.Config.DEFAULT.withQuotedCasing(TO_UPPER).withUnquotedCasing(TO_UPPER).withQuoting(DOUBLE_QUOTE)

  val typeFactory = new JavaTypeFactoryImpl()

  def parseQuery(query: String): SqlNode = {
    val parser = parserConfig.parserFactory().getParser(new StringReader(query))
    parser.setIdentifierMaxLength(parserConfig.identifierMaxLength())
    parser.setUnquotedCasing(parserConfig.unquotedCasing())
    parser.parseSqlStmtEof()
  }


  val catalogReader = {
    val readerConfig = CalciteConnectionConfig.DEFAULT.set(CalciteConnectionProperty.CASE_SENSITIVE, "false").set(CalciteConnectionProperty.CONFORMANCE, SnowflakeSqlDialect.DEFAULT_CONTEXT.conformance().toString)

    new CalciteCatalogReader(
      CalciteSchema.createRootSchema(false),
      Collections.emptyList(),
      new JavaTypeFactoryImpl(),
      readerConfig
    )
  }

  val validator =  {
    val allOperators = SqlOperatorTables.chain( SqlStdOperatorTable.instance())

    SqlValidatorUtil.newValidator(
      allOperators,
      catalogReader,
      new JavaTypeFactoryImpl(),
      SqlValidator.Config.DEFAULT.withConformance(SnowflakeSqlDialect.DEFAULT_CONTEXT.conformance())
    )
  }
  def validate(sqlNode: SqlNode): SqlNode =
    validator.validate(sqlNode)


  def prettyPrint(sqlNode: SqlNode): String = {
    val writerConfig = SqlWriterConfig
      .of()
      .withDialect(new SnowflakeSqlDialect(SnowflakeSqlDialect.DEFAULT_CONTEXT))
      .withQuoteAllIdentifiers(false)
      .withClauseEndsLine(true)
      .withLineFolding(SqlWriterConfig.LineFolding.TALL)
      .withIndentation(2)
      .withCaseClausesOnNewLines(true)
      .withClauseStartsLine(true)
      .withClauseEndsLine(true)

    val printer = new SqlPrettyWriter(writerConfig)

    printer.format(sqlNode)
  }

  def convertSqlToRel(sqlNode: SqlNode): RelNode = {
    val noopViewExpander: RelOptTable.ViewExpander = (_, _, _, _) => null

    val cluster: RelOptCluster = {
      val planner = new VolcanoPlanner()
      RelOptCluster.create(planner, new RexBuilder(typeFactory))
    }

    val sqlToRelConverter =
      new SqlToRelConverter(
        noopViewExpander, validator, catalogReader, cluster, StandardConvertletTable.INSTANCE, SqlToRelConverter.CONFIG
      )

    sqlToRelConverter.convertQuery(validatedNode, false, true).rel
  }
}
