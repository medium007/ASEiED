import com.jwszol.JoinJob
import org.scalatest.{FunSpec, GivenWhenThen}

/**
  * Created by jwszol on 12/06/17.
  */
class JoinJobTest extends FunSpec with GivenWhenThen {

  describe("JoinJobTest") {
    var jj = new JoinJob()
    jj.joinData
  }

}
