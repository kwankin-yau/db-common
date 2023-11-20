/** *****************************************************************************
 * Copyright (c) 2019, 2020 lucendar.com.
 * All rights reserved.
 *
 * Contributors:
 * KwanKin Yau (alphax@vip.163.com) - initial API and implementation
 * ******************************************************************************/
package com.lucendar.common.db.rest

import com.lucendar.common.types.rest.Pagination
import com.lucendar.common.utils.StringUtils
import info.gratour.common.error.ErrorWithCode
import org.springframework.core.MethodParameter
import org.springframework.web.bind.support.WebDataBinderFactory
import org.springframework.web.context.request.NativeWebRequest
import org.springframework.web.method.support.{HandlerMethodArgumentResolver, ModelAndViewContainer}

class PaginationResolver(maxLimit: Int) extends HandlerMethodArgumentResolver {
  override def supportsParameter(parameter: MethodParameter): Boolean = {
    parameter.getParameterType.equals(classOf[Pagination])
  }

  override def resolveArgument(parameter: MethodParameter, mavContainer: ModelAndViewContainer, webRequest: NativeWebRequest, binderFactory: WebDataBinderFactory): AnyRef = {
    val map = webRequest.getParameterMap

    var limit: java.lang.Integer = null
    var page: java.lang.Integer = null

    map.keySet().forEach(key => {
      val values = map.get(key)
      if (values.nonEmpty) {
        val v = values(0)
        key match {
          case QueryParamsResolver.PARAM_LIMIT =>
            limit = StringUtils.tryParseInt(v)
            if (limit == null) throw ErrorWithCode.invalidParam(QueryParamsResolver.PARAM_LIMIT + "[0," + maxLimit + "]")
            if (limit < 0 || limit > maxLimit) throw ErrorWithCode.invalidParam(QueryParamsResolver.PARAM_LIMIT + "[0," + maxLimit + "]")

          case QueryParamsResolver.PARAM_PAGE =>
            page = StringUtils.tryParseInt(v)
            if (page == null) throw ErrorWithCode.invalidParam(QueryParamsResolver.PARAM_PAGE)
            if (page < 0) throw ErrorWithCode.invalidParam(QueryParamsResolver.PARAM_PAGE)

          case _ =>
        }
      }
    })

    if (limit != null && page != null) {
      new Pagination(limit, page)
    } else
      null
  }
}
