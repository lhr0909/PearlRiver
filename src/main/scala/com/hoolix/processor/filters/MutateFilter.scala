package com.hoolix.pipeline.filter

import com.hoolix.pipeline.core.{Context, ContextMark, Filter, FilterConfig}

case class MutateFilter(cfg:FilterConfig, var cmd_configs: Seq[(String,Seq[Any])] = Seq()) extends Filter{
  /**
    * Seq((Context) => Any)
    */
  val cmds = cmd_configs.flatMap { case (op, param_list) =>
    param_list.flatMap { param_line =>

      //remove field
      if (Seq("remove").contains(op)) {
        val field_name = param_line.asInstanceOf[String]
        op match {
          case "remove" => Seq(
            (ctx:Context) => {
              ctx.del((cfg.pool, field_name))
            }
          )
        }
      }
      //mark record drop/pass
      else if (Seq("mark").contains(op)) {
        val mark = param_line.asInstanceOf[String]
        Seq(
          mark match {
            case "pass" => (ctx:Context) => ctx.mark = ContextMark.Pass
            case "drop" => (ctx:Context) => ctx.mark = ContextMark.Drop
          }
        )
      }
      else {
        param_line.asInstanceOf[Map[String,Any]].map { case (field_name, value) =>
          (op, value) match {

            //create field from constant
            case ("create",  value:String)             =>

              (ctx:Context) => {
                if (!ctx.has((cfg.pool, field_name)))
                  ctx.put((cfg.pool, field_name), value)
              }

            //insert field from constant
            case ("insert",  value:String)             =>

              (ctx:Context) => {
                ctx.put((cfg.pool, field_name), value)
              }

            //rename field name
            case ("rename",  newname:String)           =>

              (ctx:Context) => {
                ctx.del((cfg.pool, field_name)) match {
                  case Some(value) => ctx.put((cfg.pool, newname), value)
                  case _ =>
                }
              }

            //create field from another field
            case ("create",  from_field:Map[String,String]) =>

              val (zone, field) = from_field.toSeq(0);

              (ctx: Context) => {
                if (!ctx.has((cfg.pool, field_name)))
                  ctx.put((cfg.pool, field_name), ctx.get((zone, field), ""))
              }

            //create field from operation
            case ("create",  triple:Seq[Any]) =>

              val op_cfg = triple.head

              //TODO add type support
              val op = op_cfg match {

                case "+" => (a:Any, b:Any) => a.toString.toInt + b.toString.toInt
                case "-" => (a:Any, b:Any) => a.toString.toInt - b.toString.toInt
                case "*" => (a:Any, b:Any) => a.toString.toInt * b.toString.toInt
                case "/" => (a:Any, b:Any) => a.toString.toInt / b.toString.toInt
                case "%" => (a:Any, b:Any) => a.toString.toInt % b.toString.toInt

                case ".+" => (a:Any, b:Any) => a.toString.toDouble + b.toString.toDouble
                case ".-" => (a:Any, b:Any) => a.toString.toDouble - b.toString.toDouble
                case ".*" => (a:Any, b:Any) => a.toString.toDouble * b.toString.toDouble
                case "./" => (a:Any, b:Any) => a.toString.toDouble / b.toString.toDouble
                case ".%" => (a:Any, b:Any) => a.toString.toDouble % b.toString.toDouble

                case _   => throw new IllegalArgumentException(s"unsupported operator $op_cfg")
              }

              val getters = triple.tail.map {
                case (name:List[String]) =>
                  val x_name = name.head
                  (ctx:Context) =>  ctx.get(x_name)
                case (pair:Map[String,String]) =>
                  val (zone, x_name) = pair.toSeq.head
                  (ctx:Context) =>  ctx.get((zone, x_name))
                case (s)                      =>
                  (ctx:Context) =>  Some(s)
              }


              (ctx: Context) => {
                val values = getters.flatMap(_(ctx))
                if (values.size > 1) {
                  val value = values.reduce(op)
                  ctx.put((cfg.pool,field_name), value.toString)
                }
              }

            //insert field from another field
            case ("insert",  from_field:Map[String,String]) =>

              val (zone, field) = from_field.toSeq(0);

              (ctx:Context) => {
                ctx.put((cfg.pool, field_name), ctx.get((zone, field), ""))
              }

            //replace field value
            case ("replace", value:List[Any])       =>

              val  pattern = value(0).toString

              value(1) match {
                case (from_const: String) =>
                  (ctx:Context) => {
                    ctx.get((cfg.pool, field_name)) match {
                      case Some(old_value) => ctx.put((cfg.pool, field_name), old_value.replaceAll(pattern, from_const))
                      case _ =>
                    }
                  }
                case (from_field: Map[String, String]) =>

                  val (zone, field) = from_field.toSeq(0);

                  (ctx:Context) => {
                    val new_value = ctx.get((zone, field), null)
                    val old_value = ctx.get((cfg.pool, field_name))
                    if (new_value != null && old_value.isDefined)
                      ctx.put((cfg.pool, field_name), old_value.get.replaceAll(pattern, new_value))
                  }
              }
            }
          }
        }
      }
  }

  override def handle(ctx: Context): Either[Throwable, Iterable[(String,Any)]] = {
    cmds.foreach(_(ctx))
    Right(Seq())
  }
}
