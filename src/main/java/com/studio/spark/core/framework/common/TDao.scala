package com.studio.spark.core.framework.common

import com.studio.spark.core.framework.util.EnvUtil

trait TDao {

    def readFile(path:String) = {
        EnvUtil.take().textFile(path)
    }
}
