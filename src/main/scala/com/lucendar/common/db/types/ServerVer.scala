package com.lucendar.common.db.types

/**
 * （数据库）服务端版本号
 *
 * @param primary 主版本号
 * @param minor 次版本号
 */
case class ServerVer(primary: Int, minor: Int) {

  /**
   * 比较两个版本号
   *
   * @param ver 另一个版本号
   * @return 比 `ver` 高版本时，返回 1 ；比 `ver` 低版本时，返回 -1 ；相等时，返回 0 。
   */
  def compare(ver: ServerVer): Int = {
    if (primary > ver.primary)
      1
    else if (primary < ver.primary)
      -1
    else {
      if (minor > ver.minor)
        1
      else if (minor < ver.minor)
        -1
      else
        0
    }
  }

}

object ServerVer {
  def parse(v: String): ServerVer = {
    val p = v.indexOf('.'.toChar)
    if (p < 0)
      ServerVer(p, 0)
    else {
      val primary = v.substring(0, p).toInt
      var minor: Int = 0

      val p_dot = v.indexOf('.', p + 1)
      val p_hyp = v.indexOf('-', p + 1)
      if (p_dot > 0) {
        if (p_hyp > 0) {
          val p2 = Math.min(p_dot, p_hyp)
          minor = v.substring(p + 1, p2).toInt
        } else
          minor = v.substring(p + 1, p_dot).toInt
      } else if (p_hyp > 0) {
        minor = v.substring(p + 1, p_hyp).toInt
      } else {
        // last part is minor

        if (p + 1 < v.length) {
          minor = v.substring(p + 1).toInt
        } // else minor = 0
      }

      ServerVer(primary, minor)
    }
  }
}
