/* *********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * ********************************************************************************************************************
 */
package eu.stratosphere.tpch.query

import scala.language.reflectiveCalls

import eu.stratosphere.scala._
import eu.stratosphere.scala.operators._

import eu.stratosphere.tpch.schema._
import org.joda.time.DateTime

/**
 * Original query:
 *
 * {{{
 * select
 * 	ps_partkey,
 * 	sum(ps_supplycost * ps_availqty) as value
 * from
 * 	partsupp,
 * 	supplier,
 * 	nation
 * where
 * 	ps_suppkey = s_suppkey
 * 	and s_nationkey = n_nationkey
 * 	and n_name = ':NATION'
 * group by
 * 	ps_partkey having
 * 		sum(ps_supplycost * ps_availqty) > (
 * 			select
 * 				sum(ps_supplycost * ps_availqty) * :FRACTION
 * 			from
 * 				partsupp,
 * 				supplier,
 * 				nation
 * 			where
 * 				ps_suppkey = s_suppkey
 * 				and s_nationkey = n_nationkey
 * 				and n_name = ':NATION'
 *		)
 * order by
 * 	value desc;
 * }}}
 *
 * @param dop Degree of parallism
 * @param inPath Base input path
 * @param outPath Output path
 * @param nation Query parameter `NATION`
 * @param fraction Query parameter `FRACTION`
 */
class TPCHQuery11(dop: Int, inPath: String, outPath: String, nation: String, fraction: Double) extends TPCHQuery(11, dop, inPath, outPath) {

  def plan(): ScalaPlan = {

    val nation = Nation(inPath)
    val supplier = Supplier(inPath)
    val partsupp = PartSupp(inPath)

    val basevalue = nation  filter {
      (x) => x.name == this.nation
    } join supplier where { _.nationKey } isEqualTo { _.nationKey } map {
      (n, s) => (s.suppKey)
    } join partsupp where { x => x } isEqualTo { _.suppKey } map {
      (x, y) => y.supplyCost * y.availQty
    } reduce {
      _ + _
    } map {
      _ * fraction
    }

    val e2 = nation filter {
      (x) => x.name == this.nation
    } join supplier where { _.nationKey } isEqualTo { _.nationKey } map {
      (n, s) => (s.suppKey)
    } join partsupp where { x => x } isEqualTo { _.suppKey } map {
      (x, y) => (y.partKey, y.supplyCost * y.availQty)
    } groupBy (_._1) reduce {
      (x, y) => (x._1, y._2 + y._2)
    } cross basevalue flatMap {
      (x, y) => if (x._2 > y) Seq(x) else Seq()
    }
    // TODO: sort e4 on (_2 desc)

    val expression = e2.write(s"$outPath/query11.result", DelimitedOutputFormat(x => "%d|%f".format(x._1, x._2)))

    val plan = new ScalaPlan(Seq(expression), queryName)
    plan.setDefaultParallelism(dop)

    plan
  }
}