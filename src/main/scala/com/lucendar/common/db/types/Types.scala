/** *****************************************************************************
 * Copyright (c) 2019, 2021 lucendar.com.
 * All rights reserved.
 *
 * Contributors:
 * KwanKin Yau (alphax@vip.163.com) - initial API and implementation
 * ***************************************************************************** */
package com.lucendar.common.db.types

import com.lucendar.common.db.jdbc.StatementBinder

import java.sql.PreparedStatement

object Types {

  trait MultiBinding[T] {
    def bind(entry: T, binder: StatementBinder): Unit
  }

  trait MultiSetting[T] {
    def setParams(entry: T, ps: PreparedStatement): Unit
  }
}
