import com.jwszol.SortingJob
import org.scalatest.{FunSpec, GivenWhenThen}
import org.apache.log4j.Logger
import org.apache.log4j.Level


class SortingJobTest extends FunSpec with GivenWhenThen {
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  describe("SortingJobTest") {
    val sj = new SortingJob
    sj.selectionSort
    sj.sparkPureSort

  }

}
