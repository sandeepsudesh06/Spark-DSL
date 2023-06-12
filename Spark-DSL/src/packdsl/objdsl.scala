
package packdsl
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
object objdsl {

	def main(args:Array[String]):Unit={
			println("====started====")
			val conf = new SparkConf().setAppName("first").setMaster("local[*]")
			val sc = new SparkContext(conf)
			sc.setLogLevel("ERROR")
			val spark = SparkSession.builder().getOrCreate()
			import spark.implicits._
			//
			//			val df = spark
			//			.read
			//			.format("csv")
			//			.option("header","true")
			//			.load("file:///E:/data/dt.txt")
			//
			//			df.show()
			//			val filterdf = df.filter(col("category")==="Exercise")
			//			filterdf.show()
			//			
			//			val df1 = df.select("tdate", "category")
			//			df1.show()

			//			val df2 = df.drop("tdate", "category").show()



			//			val df1 = df.filter(col("category")==="Exercise")
			//			df1.show()
			//
			//
			//
			//			//  Multi Column filter  and 
			//
			//			val df2 = df.filter(  
			//					col("category")==="Exercise"
			//					&&		//and operator
			//					col("spendby") === "cash"
			//					)
			//
			//			df2.show()
			//
			//
			//			//  Multi Column filter  or
			//
			//			val df3 = df.filter(  
			//					col("category")==="Exercise"
			//					||                  //or operator
			//					col("spendby") === "cash"
			//					)
			//
			//			df3.show()
			//
			//
			//
			//			//  Multi value filter
			//
			//			val df4 = df.filter(col("category") isin ("Exercise","Team Sports"))
			//
			//			df4.show()

			//			//Task-01
			//			val like = df.filter(col("product") like ("%Gymnastics%"))
			//			like.show()
			//
			//			//Task-02
			//			val json = spark
			//			.read
			//			.format("json")
			//			.load("file:///E:/data/devices.json")
			//
			//			json.show()
			//
			//			val df1 = json.select(col("device_id"), col("device_name")).show()
			//
			//			val df2 = json.drop(col("temp")).show()
			//
			//			val df3 = json.filter("lat>40").show()
			//
			//			val df4 = json.filter("long<40").show()
			//
			//			val df5 = json.filter(col("long")>"40" && col("temp")<"30").show()
			//
			//			val df6 = json.filter(col("long")>"40" || ("temp")>"=20").show()
			//
			//			val df7 = json.filter("device_id>20").show()
			//
			//			val df8 = json.filter(col("device_name") like ("%am%")).show()
			//
			//


			/*val df = spark
			.read
			.format("csv")
			.option("header","true")
			.load("file:///E:/data/dt.txt")
			df.show()*/

			//			println
			//			println("======one column filter=====")
			//			println


			//			val df1 = df.filter(col("category")==="Exercise")
			//			df1.show()
			//
			//			println
			//			println("======Multi column filter=====")
			//			println		
			//
			//			val df2 = df.filter(col("category")==="Exercise" && col("spendby")==="cash")
			//			df2.show()
			//
			//			println
			//			println("======Multi or  filter=====")
			//			println					
			//
			//			val df3 = df.filter(col("category")==="Exercise" || col("spendby")==="cash")
			//			df3.show()		
			//
			//
			//
			//			println
			//			println("======Multi value=====")
			//			println					
			//
			//			val df4 = df.filter(col("category") isin ("Exercise","Team Sports"))
			//			df4.show()
			//
			//			println
			//			println("======like filter=====")
			//			println		
			//
			//
			//			val df5 = df.filter(col("product") like "%Gymnastics%")
			//			df5.show()
			//
			//			println
			//			println("======Not filter=====")
			//			println			
			//
			//
			//
			//			val df6 = df.filter(!(col("category")==="Exercise") && col("spendby")==="cash")
			//			df6.show()
			//
			//			println
			//			println("======null filter=====")
			//			println					
			//
			//
			//			val df7 = df.filter(col("product") isNull )
			//			df7.show()
			//
			//
			//			println
			//			println("======Not null filter=====")
			//			println					
			//
			//
			//			val df8 = df.filter(col("product") isNotNull)
			//			df8.show()





			//			val df1 = df.selectExpr(
			//			                          "*",
			//			                          "case when spendby='cash' then 1 else 0 end as status"
			//			)
			//			
			//			df1.show()
			//			



			//			val df1 = df.selectExpr(
			//					"id",
			//					"product",
			//					"lower(category) as lower"
			//					)
			//
			//			df1.show()

			//		val df2 = df.selectExpr(
			//		      "amount",
			//		      "upper(category) as up_category",
			//		      "lower(product) as low_product",
			//		      "case when spendby = 'cash' then 1 else 0 end as status"
			//		      )
			//		      
			//		df2.show()
			//			

			//			val df3 = df.selectExpr(
			//			     "id",
			//			     "tdate",
			//			     "split(tdate, '-')[2] as year",
			//			     "upper(category) as up_category",
			//			     "lower(product)as low_product",
			//			     "spendby",
			//			     "case when spendby = 'cash' then 1 else 0 end as status",
			//			     "concat (category, '~' , 'zeyo') as condata)"
			//			)
			//			
			//			df3.show()


			/*		val df4 = df.selectExpr(
			    "id",
			    "split(tdate, '-')[2] as year",
			    "amount",
			    "category",
			    "product",
			    "spendby"

				)

				df4.show()


			 */



			/*		val df = spark
			.read
			.format("csv")
			.option("header","true")
			.load("file:///E:/data/dt.txt")
			df.show()




			//task1


			val df5 = df.withColumn("tdate", expr("split(tdate,'-')[2]"))
			            .withColumnRenamed("tdate","year")

					df5.show()
			 */






			/*	val df = spark
			.read
			.format("csv")
			.load("file:///E:/data/datatxns.txt")
			df.show()

			val fildf = df.filter(col("_c1")==="Gymnastics")

			fildf.show()

			val Status = fildf.withColumn("status", expr("case when _c2 like 'Gym%' then 'yes' else 'no' end"))

			Status.show()
			 */


			/*		val df = spark
			.read
			.format("csv")
			.option("header", "true")
			.load("file:///E:/data/dt.txt")
			df.show()


			val df1 = df
			.withColumn("category",expr("upper(category)"))
			.withColumn("status", expr("case when spendby='cash' then 1 else 0 end"))


			df1.show()


			 */
			/*			val cust = spark
			.read
			.format("csv")
			.option("header", "true")
			.load("file:///E:/data/cust.csv")
			cust.show()

			val prod = spark
			.read
			.format("csv")
			.option("header", "true")
			.load("file:///E:/data/prod.csv")
			prod.show()

					println
					println("============Inner Join============")
			    println

			    val innerjoin = cust.join(prod, Seq("id"), "inner")
			    innerjoin.show()


						println
					println("============Left Join============")
			    println

			    val leftjoin = cust.join(prod, Seq("id"), "left")
			    leftjoin.show()

			    	println
					println("============right Join============")
			    println

			    val rightjoin = cust.join(prod, Seq("id"), "right")
			    rightjoin.show()

			    	println
					println("============full Join============")
			    println

			    val fulljoin = cust.join(prod, Seq("id"), "full")
			    fulljoin.show()

			 */



			/*	val source = spark.read.format("csv").option("header","true").load("file:///E:/data/source.csv")
			source.show()

			val target = spark.read.format("csv").option("header","true").load("file:///E:/data/target.csv").withColumnRenamed("name","name1")


			target.show()


			val full = source.join( target ,Seq("id") , "full" ).orderBy("id")

			full.show()



			val match_mis = full.withColumn("comment", expr("case when name=name1 then 'Match' else 'Mismatch' end"))

			match_mis.show()


			val remvmatch = match_mis.filter(! (col("comment")==="Match"))

			remvmatch.show()




			val finaldfpre = remvmatch.withColumn("comment", expr("case when name1 is null then 'New in Source' when name is null then 'New in Target' else comment end"))


			finaldfpre.show()




			val finaldf = finaldfpre.drop("name","name1")
			finaldf.show()
			 */


/*

			val cust = spark.read.format("csv")
			.option("header","true")
			.load("file:///E:/data/cust.csv")

			cust.show()

			val prod = spark.read.format("csv")
			.option("header","true")
			.load("file:///E:/data/prod.csv")

			prod.show()

			println
			println("======inner join=======")
			println



			val inner  = cust.join(prod,Seq("id"),"inner")
			inner.show()


			println
			println("======left join=======")
			println			

			val left  = cust.join(prod,Seq("id"),"left")
			left.show()		


			println
			println("======right join=======")
			println		

			val right  = cust.join(prod,Seq("id"),"right")
			right.show()					


			println
			println("======full join=======")
			println			

			val full  = cust.join(prod,Seq("id"),"full")
			full.show()						



			println
			println("======left anti join=======")
			println

			val left_anti  = cust.join(prod,Seq("id"),"left_anti")
			left_anti.show()						



			println
			println("======cross join=======")
			println


			val cross = cust.crossJoin(prod)
			cross.show()


*/
			
			
			
			/*
			

			val df = spark.read.format("csv")
			.option("header","true")
			.load("file:///E:/data/agg1.csv")

			df.show()


			val aggdf = df.groupBy("name","product")
			.agg(
					sum("amt").cast(IntegerType).as("total"),
					count("amt").as("cnt")
					)
			.orderBy(col("total") desc)


			aggdf.show()


			df.createOrReplaceTempView("df")


			val finald = spark.sql("select name,product,cast(sum(amt) as int) as total, count(amt) as cnt from df group by name,product order by total")

			finald.show()
			
			*/
			
			
			
			
			
			// AWS - S3 Read

/*			val df = spark
			.read
			.format("json")
			.option("fs.s3a.access.key","AKIAS3H27Y6URIBF3P4T")
			.option("fs.s3a.secret.key","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")
			.load("s3a://liyabuck/devices.json")


			df.show()
*/
			
			
			
			
			
			


			val df1 = spark.read.format("csv")
			.option("header","true")
			.load("file:///E:/data/d1.csv")

			df1.show()


			val df2 = spark.read.format("csv")
			.option("header","true")
			.load("file:///E:/data/d2.csv")

			df2.show()


			val df3 = spark.read.format("csv")
			.option("header","true")
			.load("file:///E:/data/d3.csv")

			df3.show()


			
			
			//SANDEEP SUDESH KUMAR
			
			val joindf1 = df1.join(df2,Seq("id"),"left")
			.join(df3,Seq("id"),"left")


			joindf1.show()


			val joinwith = joindf1
			.withColumn("salary",expr("case when salary is null then 0 else salary end"))
			.withColumn("salary1",expr("case when salary1 is null then 0 else salary1 end"))
			.withColumn("ns", expr("salary+salary1"))


			joinwith.show()


			val finaldf = joinwith.drop("salary","salary1")
			.withColumnRenamed("ns", "salary")

			finaldf.show()        	

			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
	}

}








































