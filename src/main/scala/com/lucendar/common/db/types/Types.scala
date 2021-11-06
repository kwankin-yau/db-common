/** *****************************************************************************
 * Copyright (c) 2019, 2021 lucendar.com.
 * All rights reserved.
 *
 * Contributors:
 * KwanKin Yau (alphax@vip.163.com) - initial API and implementation
 * ***************************************************************************** */
package com.lucendar.common.db.types

import com.lucendar.common.db.jdbc.StatementBinder

object Types {

  type MultiBinding[T] = (T, StatementBinder) => Unit

}
