/** *****************************************************************************
 * Copyright (c) 2019, 2021 lucendar.com.
 * All rights reserved.
 *
 * Contributors:
 * KwanKin Yau (alphax@vip.163.com) - initial API and implementation
 * ***************************************************************************** */
package com.lucendar.common.db.jdbc

import scala.reflect.macros.whitebox

object StatementBinderMarcos {

  def bind_impl(c: whitebox.Context)(values: c.Expr[Any]*): c.Expr[Unit] = {
    import c.universe._


    val cl = c.prefix
    val list = values.map(expr => {
      q"""
        $cl.set($expr);
       """
    }).toList

    c.Expr[Unit](q"{..$list}")
  }

}
