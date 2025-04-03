/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka

import org.apache.kafka.common.ElectionType
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.requests.ElectLeadersRequest
import scala.jdk.CollectionConverters._

/**
 * package object api { implicit final class ... } 这种写法在 Scala 中非常常见，主要用于扩展现有类的功能，提供隐式转换和增强方法。
 * 好处：
 *  扩展现有类：通过 implicit final class，可以在不修改原有类的情况下为其添加新的方法。这对于第三方库的类尤其有用，因为你不具备修改它们的源代码的能力。
 *  隐式转换：implicit 关键字使得编译器可以在需要时自动进行类型转换。这可以简化代码，提高可读性和表达力
 *  封装和组织：package object 是一个特殊的对象，属于特定的包。它可以包含隐式类、隐式方法、常量、辅助函数等。
 *    通过将这些内容放在 package object 中，可以更好地组织和封装代码，使其更模块化。
 *  统一命名空间：package object 提供了一个统一的命名空间，使得在整个包中都可以访问定义在其中的隐式类和方法，而不需要额外的导入语句。
 *  作用范围
 *    包内可见：定义在 package object 中的隐式类和方法在整个包内可见。这意味着包内的任何类或对象都可以直接使用这些隐式类和方法，而不需要显式的导入。
 *    跨包使用：如果需要在其他包中使用 package object 中定义的隐式类和方法，可以通过显式的导入来实现。例如：import api._
 *    优先级：隐式转换的优先级受导入顺序和隐式查找规则的影响。通常，定义在 package object 中的隐式类和方法具有较高的优先级，因为它们在整个包内可见。
 * 示例：假设我们有一个 String 类，我们想为其添加一个 toCamelCase 方法：
 *  package object api {
 *    implicit final class StringOps(val s: String) extends AnyVal {
 *      def toCamelCase: String = s.split(" ").map(_.capitalize).mkString
 *    }
 *  }
 *  // 在同一个包中使用
 *  object Test extends App {
 *    val str = "hello world"
 *    println(str.toCamelCase) // 输出 "HelloWorld"
 *  }
 *  在这个例子中，StringOps 是一个隐式类，它为 String 类添加了一个 toCamelCase 方法。
 *  由于 StringOps 定义在 package object api 中，因此在同一个包内的任何地方都可以直接使用 toCamelCase 方法，而不需要额外的导入。
 * 总结：
 *  package object api { implicit final class ... } 这种写法通过隐式类扩展了现有类的功能，提供了隐式转换，增强了代码的可读性和表达力。
 *  同时，package object 作为一个统一的命名空间，使得这些扩展在整个包内可见，便于组织和封装代码。
 */
package object api {
  implicit final class ElectLeadersRequestOps(val self: ElectLeadersRequest) extends AnyVal {
    def topicPartitions: Set[TopicPartition] = {
      if (self.data.topicPartitions == null) {
        Set.empty
      } else {
        self.data.topicPartitions.asScala.iterator.flatMap { topicPartition =>
          topicPartition.partitions.asScala.map { partitionId =>
            new TopicPartition(topicPartition.topic, partitionId)
          }
        }.toSet
      }
    }

    def electionType: ElectionType = {
      if (self.version == 0) {
        ElectionType.PREFERRED
      } else {
        ElectionType.valueOf(self.data.electionType)
      }
    }
  }
}
