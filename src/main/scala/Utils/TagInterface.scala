package Utils

/**
  *
  * Description:xxxx<br/>
  *
  * Copyright(c)<br/>
  *
  * All rights reserved.
  *
  * @author李海畅
  * @version:1.0
  *
  */
/**
  * 标签接口
  */
trait TagInterface {
    def makeTags(args:Any*):List[(String,Int)]
}
