package com.lucendar.common.db.jdbc

import com.google.gson.Gson
import com.lucendar.common.db.schema.FieldDataType
import com.lucendar.common.db.types.Reflections
import com.lucendar.common.utils.DateTimeUtils
import com.lucendar.common.utils.DateTimeUtils.BeijingConv
import com.typesafe.scalalogging.Logger
import info.gratour.common.error.{ErrorWithCode, Errors}
import org.springframework.jdbc.core.PreparedStatementSetter

import java.io.ByteArrayInputStream
import java.sql.{CallableStatement, PreparedStatement, Timestamp, Types}
import java.time.{Instant, LocalDate, LocalDateTime, LocalTime, OffsetDateTime, ZoneId}

class StatementBinder(val st: PreparedStatement) {

  import StatementBinder.logger

  var idx = 0

  def restart(): StatementBinder = {
    idx = 0
    this
  }

  def setNull(sqlType: Int): Unit = {
    idx += 1
    st.setNull(idx, sqlType)
  }


  def setBool(value: Boolean): Unit = {
    idx += 1
    st.setBoolean(idx, value)
  }

  def setBoolObject(value: java.lang.Boolean): Unit =
    if (value != null)
      setBool(value)
    else
      setNull(Types.BOOLEAN)

  def setBoolOpt(value: Option[Boolean]): Unit =
    if (value.isDefined)
      setBool(value.get)
    else {
      idx += 1
      st.setNull(idx, Types.BOOLEAN)
    }


  def setShort(value: Short): Unit = {
    idx += 1
    st.setShort(idx, value)
  }

  def setShort(value: Int): Unit = {
    idx += 1
    st.setShort(idx, value.toShort)
  }

  def setShortObject(value: java.lang.Short): Unit =
    if (value != null)
      setShort(value)
    else
      setNull(Types.SMALLINT)

  def setShortUseInteger(value: java.lang.Integer): Unit =
    if (value != null)
      setShort(value.shortValue())
    else
      setNull(Types.SMALLINT)

  def setShortOpt(value: Option[Short]): Unit =
    if (value.isDefined)
      setShort(value.get)
    else {
      idx += 1
      st.setNull(idx, Types.SMALLINT)
    }


  def setInt(value: Int): Unit = {
    idx += 1
    st.setInt(idx, value)
  }

  def setIntObject(value: java.lang.Integer): Unit =
    if (value != null)
      setInt(value)
    else
      setNull(Types.INTEGER)

  def setIntOpt(value: Option[Int]): Unit =
    if (value.isDefined)
      setInt(value.get)
    else
      setNull(Types.INTEGER)


  def setLong(value: Long): Unit = {
    idx += 1
    st.setLong(idx, value)
  }

  def setLongObject(value: java.lang.Long): Unit =
    if (value != null)
      setLong(value)
    else
      setNull(Types.BIGINT)

  def setLongOpt(value: Option[Long]): Unit =
    if (value.isDefined)
      setLong(value.get)
    else
      setNull(Types.BIGINT)


  def setSingle(value: Float): Unit = {
    idx += 1
    st.setFloat(idx, value)
  }

  def setSingleObject(value: java.lang.Float): Unit =
    if (value != null)
      setSingle(value)
    else
      setNull(Types.FLOAT)

  def setSingleOpt(value: Option[Float]): Unit =
    if (value.isDefined)
      setSingle(value.get)
    else
      setNull(Types.FLOAT)

  @inline def setFloat(value: Float): Unit = setSingle(value)
  @inline def setFloatObject(value: java.lang.Float): Unit = setSingleObject(value)
  @inline def setFloatOpt(value: Option[Float]): Unit = setSingleOpt(value)


  def setDouble(value: Double): Unit = {
    idx += 1
    st.setDouble(idx, value)
  }

  def setDoubleObject(value: java.lang.Double): Unit =
    if (value != null)
      setDouble(value)
    else
      setNull(Types.DOUBLE)

  def setDoubleOpt(value: Option[Double]): Unit =
    if (value.isDefined)
      setDouble(value.get)
    else
      setNull(Types.DOUBLE)


  def setDecimal(value: java.math.BigDecimal): Unit = {
    idx += 1
    st.setBigDecimal(idx, value)
  }

  def setString(value: String): Unit = {
    idx += 1
    st.setString(idx, value)
  }

  def setLocalDate(value: LocalDate): Unit =
    if (value != null) {
      idx += 1
      st.setObject(idx, value)
    } else
      setNull(Types.DATE)

  def setLocalTime(value: LocalTime): Unit =
    if (value != null) {
      idx += 1
      st.setObject(idx, value)
    } else
      setNull(Types.TIME)

  def setOffsetDateTime(value: OffsetDateTime): Unit =
    if (value != null) {
      idx += 1
      st.setObject(idx, value)
    } else
      setNull(Types.TIMESTAMP)

  def setTimestamp(value: Timestamp): Unit =
    if (value != null) {
      idx += 1
      st.setTimestamp(idx, value)
    } else
      setNull(Types.TIMESTAMP)

  def setTimestamp(epochMillis: java.lang.Long): Unit =
    if (epochMillis != null)
      setTimestamp(new Timestamp(epochMillis))
    else
      setNull(Types.TIMESTAMP)

  /**
   *
   * @param value datetime string in convenient date time format (with or without millis-seconds part) or ISO offset date time format.
   *              for example: '2000-01-01 00:00:00', '2000-01-01 00:00:00.000", '2000-01-01T00:00:00.000'
   */
  def setTimestamp(value: String): Unit =
    if (value != null) {
      idx +=1
      val dt = DateTimeUtils.stringToMillis(value)
      st.setTimestamp(idx, new Timestamp(dt))
    } else
      setNull(Types.TIMESTAMP)

  def setTimestampBeijing(value: LocalDateTime): Unit =
    if (value != null) {
      idx += 1
      val timestamp = new Timestamp(value.toInstant(DateTimeUtils.ZONE_OFFSET_BEIJING).toEpochMilli)
      st.setTimestamp(idx, timestamp)
    } else
      setNull(Types.TIMESTAMP)


  def setOffsetDateTime(epochMilli: java.lang.Long, zoneId: ZoneId): Unit =
    if (epochMilli != null)
      setOffsetDateTime(OffsetDateTime.ofInstant(Instant.ofEpochMilli(epochMilli), if (zoneId != null) zoneId else DateTimeUtils.DEFAULT_ZONE_ID))
    else
      setNull(Types.TIMESTAMP_WITH_TIMEZONE)

  /**
   *
   * @param beijingConvDateTime date time in format `yyyy-MM-dd HH:mm:ss`, treat as Beijing time.
   */
  def setBeijingConvOdt(beijingConvDateTime: String): Unit = {
    if (beijingConvDateTime != null)
      setOffsetDateTime(DateTimeUtils.BeijingConv.strToOdt(beijingConvDateTime))
    else
      setNull(Types.TIMESTAMP_WITH_TIMEZONE)
  }

  /**
   *
   * @param millis Epoch milli-seconds.
   */
  def setBeijingConvOdt(millis: java.lang.Long): Unit = {
    if (millis != null)
      setOffsetDateTime(OffsetDateTime.ofInstant(Instant.ofEpochMilli(millis), DateTimeUtils.ZONE_OFFSET_BEIJING))
    else
      setNull(Types.TIMESTAMP_WITH_TIMEZONE)
  }

  def setBinaryStream(stream: java.io.InputStream): Unit =
    if (stream != null) {
      idx += 1
      st.setBinaryStream(idx, stream)
    } else
      setNull(Types.BINARY)

  def setBinaryStream(stream: java.io.InputStream, length: Long): Unit =
    if (stream != null) {
      idx += 1
      st.setBinaryStream(idx, stream, length)
    } else
      setNull(Types.BINARY)

  def setBytes(bytes: Array[Byte], offset: Int, length: Int): Unit = {
      idx += 1
      st.setBytes(idx, bytes.slice(offset, offset + length));
  }

  def setBytes(bytes: Array[Byte]): Unit = {
    if (bytes != null) {
      idx += 1
      st.setBytes(idx, bytes)
    } else
      setNull(Types.BINARY)
  }

  def setIntArray(intArray: Array[Int]): Unit = {
    if (intArray != null) {
      val arr = st.getConnection.createArrayOf("INTEGER", intArray.map(Integer.valueOf))
      idx += 1
      st.setArray(idx, arr)
    } else
      setNull(Types.ARRAY)
  }

  import scala.language.experimental.macros
  import scala.reflect.runtime.universe._

  def setNullByClass(clazz: Class[_]): Unit = {
    clazz match {
      case Reflections.JBoolean =>
        setNull(Types.BOOLEAN)

      case Reflections.JByte | Reflections.JShort =>
        setNull(Types.SMALLINT)

      case Reflections.JInteger =>
        setNull(Types.INTEGER)

      case Reflections.JLong =>
        setNull(Types.BIGINT)

      case Reflections.JFloat =>
        setNull(Types.FLOAT)

      case Reflections.JDouble =>
        setNull(Types.DOUBLE)

      case Reflections.JBigDecimal =>
        setNull(Types.DECIMAL)

      case Reflections.JString =>
        setNull(Types.VARCHAR)

      case Reflections.JLocalDate =>
        setNull(Types.DATE)

      case Reflections.JLocalTime =>
        setNull(Types.TIME)

      case Reflections.JLocalDateTime =>
        setNull(Types.TIMESTAMP)

      case Reflections.JOffsetDateTime =>
        setNull(Types.TIMESTAMP_WITH_TIMEZONE)

      case Reflections.JByteArray =>
        setNull(Types.BINARY)

      case Reflections.JInputStream =>
        setNull(Types.BINARY)

      case Reflections.JIntArray =>
        setNull(Types.ARRAY)

      case _ =>
        val msg = Errors.errorMessage(Errors.UNSUPPORTED_TYPE) + "\n" + clazz.getName
        logger.error(msg)
        throw new ErrorWithCode(Errors.UNSUPPORTED_TYPE, msg)
    }
  }

  def setNull(fieldDataType: FieldDataType): Unit = {
    val typ =
      fieldDataType match {
        case FieldDataType.BOOL => Types.BOOLEAN
        case FieldDataType.SMALL_INT => Types.SMALLINT
        case FieldDataType.INT => Types.INTEGER
        case FieldDataType.BIGINT => Types.BIGINT
        case FieldDataType.TEXT => Types.VARCHAR
        case FieldDataType.DECIMAL => Types.NUMERIC
        case FieldDataType.FLOAT => Types.REAL
        case FieldDataType.DOUBLE => Types.DOUBLE
        case FieldDataType.LOCAL_DATE => Types.DATE
        case FieldDataType.LOCAL_DATETIME => Types.TIMESTAMP
        case FieldDataType.TIMESTAMP_WITH_ZONE => Types.TIMESTAMP_WITH_TIMEZONE
        case FieldDataType.BINARY => Types.BINARY
        case FieldDataType.INT_ARRAY => Types.ARRAY
      }

    setNull(typ)
  }

  def set[T](value: T, fieldDataType: FieldDataType)(implicit tag: TypeTag[T]): Unit = {
    value match {
      case boolean: Boolean =>
        setBool(boolean)
      case short: Short =>
        if (fieldDataType == FieldDataType.INT)
          setInt(short)
        else
          setShort(short)
      case int: Int =>
        if (fieldDataType == FieldDataType.SMALL_INT)
          setShort(int.toShort)
        else
          setInt(int)
      case l: Long =>
        if (fieldDataType == FieldDataType.TIMESTAMP_WITH_ZONE) {
          val odt = OffsetDateTime.ofInstant(Instant.ofEpochMilli(l), DateTimeUtils.DEFAULT_ZONE_ID)
          setOffsetDateTime(odt)
        } else
          setLong(l)
      case single: Float =>
        setSingle(single)
      case double: Double =>
        setDouble(double)
      case string: String =>
        if (fieldDataType == FieldDataType.BIGINT) {
          val l = string.toLong
          setLong(l)
        } else
          setString(string)
      case localDate: LocalDate =>
        setLocalDate(localDate)
      case localTime: LocalTime =>
        setLocalTime(localTime)
      case offsetDateTime: OffsetDateTime =>
        setOffsetDateTime(offsetDateTime)

      case jbool: java.lang.Boolean =>
        if (jbool != null)
          setBool(jbool.booleanValue())
        else
          setNull(Types.BOOLEAN)

      case jbyte: java.lang.Byte =>
        if (jbyte != null)
          setShort(jbyte.shortValue())
        else
          setNull(Types.SMALLINT)

      case jshort: java.lang.Short =>
        if (jshort != null)
          setShort(jshort.shortValue())
        else
          setNull(Types.SMALLINT)

      case jint: java.lang.Integer =>
        if (jint != null)
          setInt(jint.intValue())
        else
          setNull(Types.INTEGER)

      case jlong: java.lang.Long =>
        if (jlong != null)
          setLong(jlong.longValue())
        else
          setNull(Types.BIGINT)

      case jsingle: java.lang.Float =>
        if (jsingle != null)
          setSingle(jsingle.floatValue())
        else
          setNull(Types.FLOAT)

      case jdouble: java.lang.Double =>
        if (jdouble != null)
          setDouble(jdouble.doubleValue())
        else
          setNull(Types.DOUBLE)

      case jdec: java.math.BigDecimal =>
        setDecimal(jdec)

      case jInputStream: java.io.InputStream =>
        setBinaryStream(jInputStream)

      case bytes: Array[Byte] =>
        setBytes(bytes)

      case intArr: Array[Int] =>
        setIntArray(intArr)

      case opt: Option[_] =>
        tag.tpe match {
          case TypeRef(_, _, args) =>
            val optArgType = args.head
            if (optArgType =:= com.lucendar.common.db.types.Types.BoolType) {
              if (opt.isDefined)
                setBool(opt.get.asInstanceOf[Boolean])
              else
                setNull(Types.BOOLEAN)
            } else if (optArgType =:= com.lucendar.common.db.types.Types.ByteType) {
              if (opt.isDefined)
                setShort(opt.get.asInstanceOf[Byte])
              else
                setNull(Types.SMALLINT)
            } else if (optArgType =:= com.lucendar.common.db.types.Types.ShortType) {
              if (opt.isDefined)
                setShort(opt.get.asInstanceOf[Short])
              else
                setNull(Types.SMALLINT)
            } else if (optArgType =:= com.lucendar.common.db.types.Types.IntType) {
              if (opt.isDefined)
                setInt(opt.get.asInstanceOf[Int])
              else
                setNull(Types.INTEGER)
            } else if (optArgType =:= com.lucendar.common.db.types.Types.LongType) {
              if (opt.isDefined)
                setLong(opt.get.asInstanceOf[Long])
              else
                setNull(Types.BIGINT)
            } else if (optArgType =:= com.lucendar.common.db.types.Types.FloatType) {
              if (opt.isDefined)
                setSingle(opt.get.asInstanceOf[Float])
              else
                setNull(Types.FLOAT)
            } else if (optArgType =:= com.lucendar.common.db.types.Types.DoubleType) {
              if (opt.isDefined)
                setDouble(opt.get.asInstanceOf[Double])
              else
                setNull(Types.DOUBLE)
            } else if (optArgType =:= com.lucendar.common.db.types.Types.InputStreamType) {
              if (opt.isDefined)
                setBinaryStream(opt.get.asInstanceOf[java.io.InputStream])
              else
                setNull(Types.BINARY)
            } else
              throw new ErrorWithCode(Errors.UNSUPPORTED_TYPE, "Unsupported element type: " + optArgType.toString)
        }


      case _ =>
        val tpe = tag.tpe

        if (tpe =:= com.lucendar.common.db.types.Types.StringType)
          setNull(Types.VARCHAR)
        else if (tpe =:= com.lucendar.common.db.types.Types.LocalDateType)
          setNull(Types.DATE)
        else if (tpe =:= com.lucendar.common.db.types.Types.LocalTimeType)
          setNull(Types.TIME)
        else if (tpe =:= com.lucendar.common.db.types.Types.OffsetDateTimeType)
          setNull(Types.TIMESTAMP_WITH_TIMEZONE)
        else if (tpe =:= com.lucendar.common.db.types.Types.JBigDecimalType)
          setNull(Types.DECIMAL)
        else if (tpe =:= com.lucendar.common.db.types.Types.JCharacterType)
          setNull(Types.CHAR)
        else if (tpe =:= com.lucendar.common.db.types.Types.JBooleanType)
          setNull(Types.BOOLEAN)
        else if (tpe =:= com.lucendar.common.db.types.Types.JIntegerType)
          setNull(Types.INTEGER)
        else if (tpe =:= com.lucendar.common.db.types.Types.JShortType)
          setNull(Types.SMALLINT)
        else if (tpe =:= com.lucendar.common.db.types.Types.JByteType)
          setNull(Types.SMALLINT)
        else if (tpe =:= com.lucendar.common.db.types.Types.JFloatType)
          setNull(Types.FLOAT)
        else if (tpe =:= com.lucendar.common.db.types.Types.JDoubleType)
          setNull(Types.DOUBLE)
        else if (tpe =:= com.lucendar.common.db.types.Types.InputStreamType)
          setNull(Types.BINARY)
        else {
          if (fieldDataType == FieldDataType.TEXT) {
            if (value != null)
              setString(value.toString)
            else
              setString(null)
          } else
            throw new ErrorWithCode(Errors.UNSUPPORTED_TYPE)
        }
    }
  }

  def set[T](value: T)(implicit tag: TypeTag[T]): Unit = {
    value match {
      case boolean: Boolean =>
        setBool(boolean)
      case b: Byte =>
        setShort(b)
      case short: Short =>
        setShort(short)
      case int: Int =>
        setInt(int)
      case l: Long =>
        setLong(l)
      case single: Float =>
        setSingle(single)
      case double: Double =>
        setDouble(double)
      case string: String =>
        setString(string)

      case localDate: LocalDate =>
        setLocalDate(localDate)
      case localTime: LocalTime =>
        setLocalTime(localTime)
      case offsetDateTime: OffsetDateTime =>
        setOffsetDateTime(offsetDateTime)

      case jbool: java.lang.Boolean =>
        if (jbool != null)
          setBool(jbool.booleanValue())
        else
          setNull(Types.BOOLEAN)

      case jbyte: java.lang.Byte =>
        if (jbyte != null)
          setShort(jbyte.shortValue())
        else
          setNull(Types.SMALLINT)
      case jshort: java.lang.Short =>
        if (jshort != null)
          setShort(jshort.shortValue())
        else
          setNull(Types.SMALLINT)

      case jint: java.lang.Integer =>
        if (jint != null)
          setInt(jint.intValue())
        else
          setNull(Types.INTEGER)

      case jlong: java.lang.Long =>
        if (jlong != null)
          setLong(jlong.longValue())
        else
          setNull(Types.BIGINT)

      case jsingle: java.lang.Float =>
        if (jsingle != null)
          setSingle(jsingle.floatValue())
        else
          setNull(Types.FLOAT)

      case jdouble: java.lang.Double =>
        if (jdouble != null)
          setDouble(jdouble.doubleValue())
        else
          setNull(Types.DOUBLE)

      case jdec: java.math.BigDecimal =>
        setDecimal(jdec)

      case stream: java.io.InputStream =>
        setBinaryStream(stream)

      case opt: Option[_] =>
        tag.tpe match {
          case TypeRef(_, _, args) =>
            val optArgType = args.head
            if (optArgType =:= com.lucendar.common.db.types.Types.BoolType) {
              if (opt.isDefined)
                setBool(opt.get.asInstanceOf[Boolean])
              else
                setNull(Types.BOOLEAN)
            } else if (optArgType =:= com.lucendar.common.db.types.Types.ByteType) {
              if (opt.isDefined)
                setShort(opt.get.asInstanceOf[Byte])
              else
                setNull(Types.SMALLINT)
            } else if (optArgType =:= com.lucendar.common.db.types.Types.ShortType) {
              if (opt.isDefined)
                setShort(opt.get.asInstanceOf[Short])
              else
                setNull(Types.SMALLINT)
            } else if (optArgType =:= com.lucendar.common.db.types.Types.IntType) {
              if (opt.isDefined)
                setInt(opt.get.asInstanceOf[Int])
              else
                setNull(Types.INTEGER)
            } else if (optArgType =:= com.lucendar.common.db.types.Types.LongType) {
              if (opt.isDefined)
                setLong(opt.get.asInstanceOf[Long])
              else
                setNull(Types.BIGINT)
            } else if (optArgType =:= com.lucendar.common.db.types.Types.FloatType) {
              if (opt.isDefined)
                setSingle(opt.get.asInstanceOf[Float])
              else
                setNull(Types.FLOAT)
            } else if (optArgType =:= com.lucendar.common.db.types.Types.DoubleType) {
              if (opt.isDefined)
                setDouble(opt.get.asInstanceOf[Double])
              else
                setNull(Types.DOUBLE)
            } else if (optArgType =:= com.lucendar.common.db.types.Types.InputStreamType) {
              if (opt.isDefined)
                setBinaryStream(opt.get.asInstanceOf[java.io.InputStream])
              else
                setNull(Types.BINARY)
            } else
              throw new ErrorWithCode(Errors.UNSUPPORTED_TYPE, "Unsupported element type: " + optArgType.toString)
        }


      case _ =>
        val tpe = tag.tpe

        if (tpe =:= com.lucendar.common.db.types.Types.StringType)
          setNull(Types.VARCHAR)
        else if (tpe =:= com.lucendar.common.db.types.Types.LocalDateType)
          setNull(Types.DATE)
        else if (tpe =:= com.lucendar.common.db.types.Types.LocalTimeType)
          setNull(Types.TIME)
        else if (tpe =:= com.lucendar.common.db.types.Types.OffsetDateTimeType)
          setNull(Types.TIMESTAMP_WITH_TIMEZONE)
        else if (tpe =:= com.lucendar.common.db.types.Types.JBigDecimalType)
          setNull(Types.DECIMAL)
        else if (tpe =:= com.lucendar.common.db.types.Types.JCharacterType)
          setNull(Types.CHAR)
        else if (tpe =:= com.lucendar.common.db.types.Types.JBooleanType)
          setNull(Types.BOOLEAN)
        else if (tpe =:= com.lucendar.common.db.types.Types.JIntegerType)
          setNull(Types.INTEGER)
        else if (tpe =:= com.lucendar.common.db.types.Types.JShortType)
          setNull(Types.SMALLINT)
        else if (tpe =:= com.lucendar.common.db.types.Types.JByteType)
          setNull(Types.SMALLINT)
        else if (tpe =:= com.lucendar.common.db.types.Types.JFloatType)
          setNull(Types.FLOAT)
        else if (tpe =:= com.lucendar.common.db.types.Types.JDoubleType)
          setNull(Types.DOUBLE)
        else if (tpe =:= com.lucendar.common.db.types.Types.InputStreamType)
          setNull(Types.BINARY)
        else
          throw new ErrorWithCode(Errors.UNSUPPORTED_TYPE)
    }
  }

  def bind(values: Any*): Unit = macro StatementBinderMarcos.bind_impl


  def json(gson: Gson, value: AnyRef): Unit = {
    if (value != null)
      setString(gson.toJson(value))
    else
      setNull(Types.VARCHAR)
  }
}

object StatementBinder {
  private val logger = Logger[StatementBinder]

  def apply(ps: PreparedStatement): StatementBinder = new StatementBinder(ps)
}

class CallableStmtHandler(override val st: CallableStatement) extends StatementBinder(st) {

  def registerOutParameter(sqlType: Int): Unit = {
    idx += 1
    st.asInstanceOf[CallableStatement].registerOutParameter(idx, sqlType)
  }

  /**
   * Executes the SQL statement in this PreparedStatement object, which may be any kind of SQL statement. Some prepared
   * statements return multiple results; the execute method handles these complex statements as well as the simpler form
   * of statements handled by the methods executeQuery and executeUpdate.
   * The execute method returns a boolean to indicate the form of the first result. You must call either the method
   * getResultSet or getUpdateCount to retrieve the result; you must call getMoreResults to move to any subsequent
   * result(s).
   *
   * @return true if the first result is a ResultSet object; false if the first result is an update count or there is
   *         no result.
   */
  def execute(): Boolean =
    st.execute()

  def getBool(colIndex: Int): Boolean =
    st.getBoolean(colIndex)

  def getBoolObject(colIndex: Int): java.lang.Boolean = {
    val r = st.getBoolean(colIndex)
    if (st.wasNull())
      null
    else
      r
  }

  def getInt(colIndex: Int): Int =
    st.getInt(colIndex)

  def getIntObject(colIndex: Int): Integer = {
    val r = st.getInt(colIndex)
    if (st.wasNull())
      null
    else
      r
  }

  def getLong(colIndex: Int): Long =
    st.getLong(colIndex)

  def getLongObject(colIndex: Int): java.lang.Long = {
    val r = st.getLong(colIndex)
    if (st.wasNull())
      null
    else
      r
  }

  def getString(colIndex: Int): String =
    st.getString(colIndex)

}

object CallableStmtHandler {
  def apply(cs: CallableStatement): CallableStmtHandler = new CallableStmtHandler(cs)
}

trait StatementSetter {
  def set(binder: StatementBinder): Unit
}

class StatementSetterWrapper(setter: StatementSetter) extends PreparedStatementSetter {

  override def setValues(ps: PreparedStatement): Unit = {
    val binder = new StatementBinder(ps)
    setter.set(binder)
  }

}

object StatementSetterWrapper {
  def apply(setter: StatementSetter): StatementSetterWrapper = {
    if (setter != null)
      new StatementSetterWrapper(setter)
    else
      null
  }
}

trait StatementBinderProcessor {
  def process(binder: StatementBinder): Unit
}

