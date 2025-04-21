package io.github.antonkw

import org.apache.calcite.rel.RelNode
import org.apache.calcite.sql.SqlNode

object Fibonacci extends App {
  val fibonacci =
    """
      |WITH RECURSIVE FIBONACCI (N, FIB, NEXT_FIB) AS (
      |  -- Base case: first two Fibonacci numbers
      |  SELECT
      |    1 AS N,
      |    0 AS FIB,
      |    1 AS NEXT_FIB
      |
      |  UNION ALL
      |
      |  -- Recursive case: generate next Fibonacci number
      |  SELECT
      |    N + 1,
      |    NEXT_FIB,
      |    FIB + NEXT_FIB
      |  FROM
      |    FIBONACCI
      |  WHERE
      |    N < 10  -- Generate first 10 Fibonacci numbers
      |)
      |
      |-- Select the sequence
      |SELECT
      |  N,
      |  FIB
      |FROM
      |  FIBONACCI
      |ORDER BY
      |  N
      |""".stripMargin

  val fibonacciSqlNode: SqlNode = Calcite.parseQuery(fibonacci)

  /*
  WITH RECURSIVE `FIBONACCI` (`N`, `FIB`, `NEXT_FIB`) AS (SELECT 1 AS `N`, 0 AS `FIB`, 1 AS `NEXT_FIB`
  UNION ALL
  SELECT `N` + 1, `NEXT_FIB`, `FIB` + `NEXT_FIB`
  FROM `FIBONACCI`
  WHERE `N` < 10) SELECT `N`, `FIB`
  FROM `FIBONACCI`
  ORDER BY `N`
   */



  val validatedNode: SqlNode = Calcite.validate(fibonacciSqlNode)
  val formatted = Calcite.prettyPrint(validatedNode)
  println(formatted)

  val relationalNode: RelNode = Calcite.convertSqlToRel(validatedNode)
  val logicalPlanPrinted = relationalNode.explain()
  println(logicalPlanPrinted)
}
