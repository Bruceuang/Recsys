import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.ArrayBuffer
import scala.math._

boject CF {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
        conf.setMaster("local")
        conf.setAppName("CF_Test")
        
        val sc = new SparkContext(conf)
        val lines = sc.textFile(args(0))
        val output_path = args(1).toString
        
        val score_thd = 0.0001	//得分阈值，小于阈值的过滤
        val max_prefs_pre_user = 20	//观看列表长度，超过则截断
        val topn = 5	//每一个item最多推荐5个
        
        
        val ui_rdd = lines.map {x =>
        	val fields = x.split("\t")
            (fields(0).toString, (fields(1).toString, fields(2).toDouble))
        }.filter { x =>
            x._2._2 > score_thd
        }.groupByKey().flatMap{ x =>
            val user = x._1
            val is_list = x._2
            val is_arr = is_list.toArray
            var is_arr_len = is_arr.length
            if (is_arr_len > max_prefs_pre_user) {
                is_arr_len = max_prefs_pre_user
            }
            
            var i_us_arr = new ArrayBuffer[(String, (String, Double))]
            for (i <- 0 until is_arr_len) {
                is_us_arr += ((is_arr(i)._1, (user, is_arr(i)._2)))
            }
            i_us_arr
        }groupByKey().flatMap{ x =>
            val item = x._1
            val u_list = x._2
            val us_arr = u_list.toArray
            var sum:Double = 0
            for (i <- 0 until us_arr.length){
                sum += pow(us_arr(i)._2, 2)
            }
            sum = sqrt(sum)
            
            var u_is_arr = new ArrayBuffer[(String, (String, Double))]
            for (i <- 0 until us_arr.length){
                u_is_arr += ((us_arr(i)._1, (item, us_arr(i)._2 / sum)))
            }
            u_is_arr
        }.groupByKey()
        
        
        val ii_rdd = ui_rdd.flatMap{ x =>
            val is_arr = x._2.toArray
            var ii_s_arr = new ArrayBuffer[((String, String), Double)]
            for (i <- 0 until is_arr.length - 1){
                for (j <- i+1 until is_arr.length){
                    ii_s_arr += (((is_arr(i)._1, is_arr(j)._1), is_arr(i)._2 * is_arr(j)._2))
                    ii_s_arr += (((is_arr(j)._1, is_arr(i)._1), is_arr(i)._2 * is_arr(j)._2))
                }
            }
            
            ii_s_arr
        }.groupByKey().map{ x =>
            val ii_pair = x._1
            val s_list = x._2
            val s_arr = s_list.toArray
            val score:Double = 0.0
            for (i <- 0 until len){
                score += s_arr(i)
            }
            (ii_pair_1, (ii_pair._2, score))
        }.groupByKey().map { x =>
            val a_item = x._1
            val bs_list = x._2
            val bs_arr = bs_list.toArray.sortWith(_._2 > _._2)
            var len = bs_arr.length
            if (len > topn){
                len = topn
            }
            val s = new StringBuilder
            for (i <- until len){
                val item = bs_arr(i)._1
                val score = "%1.4" format bs_arr(i)._2
                s.append(item + ":" + score)
                s.append(",")
            }
            a_item + "\t" + s
        }.saveAsTextFile(output_path)
        }
    }
}