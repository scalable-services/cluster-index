package cluster

trait Block[K] {
  val id: String

  val MIN: Int
  val MAX: Int

  def first: K
  def last: K

  def length: Int

  def isFull(): Boolean
  def isEmpty(): Boolean

  def print(): String
}
