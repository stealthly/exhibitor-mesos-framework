/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ly.stealth.mesos.exhibitor

import java.util.regex.{Pattern, PatternSyntaxException}

import org.apache.log4j.Logger
import org.apache.mesos.Protos.Offer

import scala.util.Try
import scala.collection.JavaConversions._

trait Constraint {
  def matches(value: String, values: List[String] = Nil): Boolean
}

object Constraint {
  def apply(value: String): Constraint = {
    if (value.startsWith("like:")) Constraint.Like(value.substring("like:".length))
    else if (value.startsWith("unlike:")) Constraint.Like(value.substring("unlike:".length), negated = true)
    else if (value == "unique") Constraint.Unique()
    else if (value.startsWith("cluster")) {
      val tail = value.substring("cluster".length)
      val cluster = if (tail.startsWith(":")) Some(tail.substring(1)) else None
      Cluster(cluster)
    } else if (value.startsWith("groupBy")) {
      val tail = value.substring("groupBy".length)
      val groups = if (tail.startsWith(":")) Try(tail.substring(1).toInt).toOption.getOrElse(throw new IllegalArgumentException(s"invalid condition $value"))
      else 1

      GroupBy(groups)
    }
    else throw new IllegalArgumentException(s"Unsupported condition: $value")
  }

  def parse(constraints: String): Map[String, List[Constraint]] = {
    Util.parseList(constraints).foldLeft[Map[String, List[Constraint]]](Map()) { case (all, (name, value)) =>
      all.get(name) match {
        case Some(values) => all.updated(name, Constraint(value) :: values)
        case None => all.updated(name, List(Constraint(value)))
      }
    }
  }

  case class Like(regex: String, negated: Boolean = false) extends Constraint {
    val pattern = try {
      Pattern.compile(s"^$regex$$")
    } catch {
      case e: PatternSyntaxException => throw new IllegalArgumentException(s"Invalid $name: ${e.getMessage}")
    }

    private def name: String = if (negated) "unlike" else "like"

    def matches(value: String, values: List[String]): Boolean = negated ^ pattern.matcher(value).find()

    override def toString: String = s"$name:$regex"
  }

  case class Unique() extends Constraint {
    def matches(value: String, values: List[String]): Boolean = !values.contains(value)

    override def toString: String = "unique"
  }

  case class Cluster(value: Option[String]) extends Constraint {
    def matches(value: String, values: List[String]): Boolean = this.value match {
      case Some(v) => v == value
      case None => values.isEmpty || values.head == value
    }

    override def toString: String = "cluster" + value.map(":" + _).getOrElse("")
  }

  case class GroupBy(groups: Int = 1) extends Constraint {
    def matches(value: String, values: List[String]): Boolean = {
      val counts = values.groupBy(identity).mapValues(_.size)
      if (counts.size < groups) !counts.contains(value)
      else {
        val minCount = counts.values.reduceOption(_ min _).getOrElse(0)
        counts.getOrElse(value, 0) == minCount
      }
    }

    override def toString: String = "groupBy" + (if (groups > 1) s":$groups" else "")
  }

}

trait Constraints[T <: Constrained] {
  private val logger = Logger.getLogger(this.getClass)

  def checkConstraints(offer: Offer, task: T): Option[String] = {
    val offerAttributes = offer.getAttributesList.toList.foldLeft(Map("hostname" -> offer.getHostname)) { case (attributes, attribute) =>
      if (attribute.hasText) attributes.updated(attribute.getName, attribute.getText.getValue)
      else attributes
    }

    for ((name, constraints) <- task.constraints) {
      for (constraint <- constraints) {
        offerAttributes.get(name) match {
          case Some(attribute) =>
            if (!constraint.matches(attribute, otherTasksAttributes(name))) {
              logger.debug(s"Attribute $name doesn't match $constraint")
              return Some(s"$name doesn't match $constraint")
            }
          case None =>
            logger.debug(s"Offer does not contain $name attribute")
            return Some(s"no $name")
        }
      }
    }

    None
  }

  private def otherTasksAttributes(name: String): List[String] = tasks.flatMap(_.attribute(name)).toList

  def tasks: Traversable[T]
}

trait Constrained {
  def constraints: scala.collection.Map[String, List[Constraint]]

  def attribute(name: String): Option[String]
}
