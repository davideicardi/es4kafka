package es4kafka.modules

import es4kafka.{ServiceAppController, SingletonScope}
import net.codingwell.scalaguice.ScalaMultibinder

import scala.concurrent.duration.FiniteDuration
import scala.reflect.runtime.universe.TypeTag

object Module {
  import com.google.inject.AbstractModule
  import net.codingwell.scalaguice.ScalaModule

  abstract class Installer extends AbstractModule with ScalaModule {
    protected def configure(): Unit

    protected def newSetBinder[T: TypeTag](): ScalaMultibinder[T] = {
      ScalaMultibinder.newSetBinder[T](binder)
    }

    protected def bindModule[T <: Module : TypeTag](): Unit = {
      val modules = newSetBinder[Module]()
      modules.addBinding.to[T].in[SingletonScope]()
    }
  }
}

trait Module  {
  /**
   * Priority (from higher to lower) used to start modules.
   * When stopping modules the reversed order is used.
   * ie. the module with greater priority is started first and stopped last
   */
  val priority: Int = 0

  def start(controller: ServiceAppController): Unit

  def stop(maxWait: FiniteDuration, reason: String): Unit

  override def toString: String = this.getClass.getSimpleName
}
