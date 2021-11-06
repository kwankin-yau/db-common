/** *****************************************************************************
 * Copyright (c) 2019, 2021 lucendar.com.
 * All rights reserved.
 *
 * Contributors:
 * KwanKin Yau (alphax@vip.163.com) - initial API and implementation
 * ***************************************************************************** */
package com.lucendar.common.db

object DbUtils {

  def valuesToSqlPlaceHolders(values: Array[_]): String =
    if (values != null && !values.isEmpty)
      values.map(_ => "?").mkString(",")
    else
      null

}
