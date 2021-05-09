package cluster

trait Cache {

  def get(id: String): Block
  def save(ctx: Context): Boolean

}
