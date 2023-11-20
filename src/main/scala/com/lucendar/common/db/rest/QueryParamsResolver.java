package com.lucendar.common.db.rest;


import com.lucendar.common.db.types.Predication;
import com.lucendar.common.types.rest.Pagination;
import com.lucendar.common.utils.StringUtils;
import info.gratour.common.error.ErrorWithCode;
import org.springframework.core.MethodParameter;
import org.springframework.web.bind.support.WebDataBinderFactory;
import org.springframework.web.context.request.NativeWebRequest;
import org.springframework.web.method.support.HandlerMethodArgumentResolver;
import org.springframework.web.method.support.ModelAndViewContainer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class QueryParamsResolver implements HandlerMethodArgumentResolver {

    public static final String PARAM_LIMIT = "__limit";
    public static final String PARAM_PAGE = "__page";
    public static final String PARAM_ORDER_BY = "__orderBy";
    public static final String PARAM_FIELDS = "__fields";
//    public static final String PARAM_TOKEN = "__token";

    private final int maxLimit;

    public QueryParamsResolver(int maxLimit) {
        this.maxLimit = maxLimit;
    }

    @Override
    public boolean supportsParameter(MethodParameter parameter) {
        return parameter.getParameterType().equals(QueryParams.class);
    }

    @Override
    public Object resolveArgument(MethodParameter parameter, ModelAndViewContainer mavContainer, NativeWebRequest request, WebDataBinderFactory binderFactory) throws Exception {
        Map<String, String[]> map = request.getParameterMap();

        Integer limit = null;
        Integer page = null;
        Pagination pagination = null;
        SortColumn[] sortColumns = null;
        String[] fields = null;
        List<SearchCondition> searchConditions = new ArrayList<>();

        for (String key : map.keySet()) {
            String[] values = map.get(key);

            switch (key) {
                case PARAM_LIMIT:
                    limit = StringUtils.tryParseInt(values[0]);
                    if (limit == null)
                        throw ErrorWithCode.invalidParam(PARAM_LIMIT + "[0," + maxLimit + "]");
                    if (limit < 0 || limit > maxLimit)
                        throw ErrorWithCode.invalidParam(PARAM_LIMIT + "[0," + maxLimit + "]");
                    break;

                case PARAM_PAGE:
                    page = StringUtils.tryParseInt(values[0]);
                    if (page == null)
                        throw ErrorWithCode.invalidParam(PARAM_PAGE);
                    if (page < 0)
                        throw ErrorWithCode.invalidParam(PARAM_PAGE);
                    break;

                case PARAM_ORDER_BY:
                    sortColumns = QueryParams.parseSortColumns(values[0]);
                    break;

                case PARAM_FIELDS:
                    fields = values[0].split(",");

                default: {
                    Predication predication;
                    int index = key.lastIndexOf('~');
                    if (index > 0) {
                        String p = key.substring(index + 1);

                        switch (p) {
                            case "lt":
                                predication = Predication.LESS;
                                break;

                            case "gt":
                                predication = Predication.GREAT;
                                break;

                            case "le":
                                predication = Predication.LESS_EQUAL;
                                break;

                            case "ge":
                                predication = Predication.GREAT_EQUAL;
                                break;

                            case "in":
                                predication = Predication.IN;
                                break;

                            case "null":
                                predication = Predication.IS_NULL;
                                break;

                            case "notnull":
                                predication = Predication.NOT_NULL;
                                break;

                            case "start":
                                predication = Predication.START_WITH;
                                break;

                            case "end":
                                predication = Predication.END_WITH;
                                break;

                            case "like":
                                predication = Predication.INCLUDE;
                                break;

                            case "under":
                                predication = Predication.UNDER;
                                break;

                            default:
                                throw ErrorWithCode.invalidParam(key);
                        }

                        key = key.substring(0, index);
                    } else {
                        predication = null;
                    }
                    searchConditions.add(new SearchCondition(key, values[0], predication));
                }
            }
        }

        if (limit != null) {
            if (page == null)
                throw ErrorWithCode.invalidParam(PARAM_PAGE);

            pagination = new Pagination(limit, page);
        } else if (page != null) {
            throw ErrorWithCode.invalidParam(PARAM_LIMIT);
        }

        return new QueryParams(SearchConditions.apply(searchConditions), pagination, sortColumns, fields);
    }
}
