package com.ebctech.lkq

import org.apache.spark.sql.functions._

class Feed extends FeedFunctions {

  var lkqsolr = spark.read.format("csv").options(Map("header" -> "true","delimiter" -> "^"))
    .load("/home/spineor/Downloads/CATALOGUE/ppm_catalog_dec30/TAPS2/Amit/December_2019/LKQ_Data_Dec30.csv").select("PartNumber",
    "ProductType", "Brand", "CategoryName", "Description", "ImageURL", "ItemLevelGTIN", "Position",
    "SubCategoryName", "Fitment", "Price_Primary", "skuid", "certification", "Partslink", "condition")
    .distinct()
    .cache()

  lkqsolr = lkqsolr.withColumn("brand", regexp_replace(col("brand"),
    "Platinum Plus, Platinum Pro","Platinum Plus"))

  lkqsolr = lkqsolr.withColumn("brand", regexp_replace(col("brand"),
    "Platinum Plus, Diamond Standard","Platinum Plus"))

  lkqsolr = lkqsolr.filter(col("ImageURL").isNotNull)

  val lkqsolr1 = lkqsolr.withColumn("Fitment", stringToArrayUDF(col("Fitment")))
  val lkqsolr2 = lkqsolr1.withColumn("Fitment", explode(col("Fitment")))
  var lkqsolr3 = lkqsolr2.withColumn("Year", split(col("Fitment"), "/").getItem(0))
    .withColumn("Make", split(col("Fitment"), "/").getItem(1))
    .withColumn("Model", split(col("Fitment"), "/").getItem(2))
    .withColumn("SubModel", split(col("Fitment"), "/").getItem(3))
    .withColumn("Engine", split(col("Fitment"), "/").getItem(4))

  lkqsolr3 = lkqsolr3.select("PartNumber", "ProductType", "Brand", "CategoryName", "Description", "ImageURL",
    "ItemLevelGTIN", "Position", "SubCategoryName", "Price_Primary", "Condition", "skuid", "Year", "Make", "Model", "SubModel",
    "Engine", "certification", "Partslink")

  var lkqsolr4 = lkqsolr3.filter(col("ProductType") =!= "Wheel")

  lkqsolr4 = lkqsolr4.withColumn("Condition", conditionUDF(col("Condition"))).distinct

  println("lkqsolr4 count is " + lkqsolr4.count())
  println("lkqsolr4 PArt Number count is " + lkqsolr4.select("PartNUmber").distinct.count())

  var lkq1 = lkqsolr4.select("PartNumber", "ProductType", "Brand", "CategoryName", "Description", "ImageURL", "ItemLevelGTIN",
    "Position", "SubCategoryName", "Price_Primary", "Condition", "skuid", "certification", "Year", "Make", "Model", "SubModel", "Engine", "Partslink")

  println("lkq1 count is "+ lkq1.select("PartNumber").distinct.count)

  lkq1 = lkq1.withColumn("Liter", split(col("Engine"), "-").getItem(0))
    .withColumn("Cylinders", split(col("Engine"), "-").getItem(1))
    .drop("Engine").drop("price")

  var ppm_catalogue_price_20thFeb2020 = spark.read.format("csv")
    .option("delimiter", "^").option("header", "true")
    .load("/home/spineor/Desktop/ppm_catalogue_price_19thFeb2020/ppm_catalogue_price_20thFeb2020.csv")
    .distinct()
    .cache()

  //for fetching price
  var lkq_price = lkq1.join(ppm_catalogue_price_20thFeb2020, "PartNumber").distinct

  lkq_price = lkq_price.withColumn("brand", ManufacturerBrandUDF(col("Condition"), col("Make"), col("Brand")))

  //Applying the null brand function
  val lkqfl = lkq_price.withColumn("Brand", nullBrandsUDF(col("Brand")))

  // Create product_type and mpn column
  var lkq2 = lkqfl.withColumn("product_type", concat(col("CategoryName"), lit(" > "),
    col("SubCategoryName"), lit(" > "), col("ProductType"), lit(" > "),
    col("PartNumber")))

  var lkq3 = lkq2.withColumn("mpn", col("PartNumber"))

  //Renaming the Product Type column
  var lkq4 = lkq3.withColumnRenamed("ProductType", "ProType")

  var lkq5 = lkq4.withColumnRenamed("SubModelName", "SubModel")

  //Creating columns Engine, SubModel etc
  var submodel_engine = lkq5.withColumn("Engine", concat(col("Cylinders"), lit("C "),
    col("Liter"), lit("L "))).drop("Cylinders", "Liter")

  submodel_engine = submodel_engine.groupBy("mpn").agg(collect_list("SubModel").as("SubModel"),
    collect_list("Engine").as("Engine"))
  submodel_engine = submodel_engine.withColumn("SubModel", arrayToStringUDFforLKQ(col("SubModel")))
    .withColumn("Engine", arrayToStringUDFforLKQ(col("Engine")))

  lkq5 = lkq5.drop("SubModel")

  //Joining submodel_engine
  val lkq6 = lkq5.join(submodel_engine, "mpn").distinct

  val lkq7 = lkq6.withColumn("description", descriptionUDF(col("Certification"),
    col("Description"), col("SubModel"),
    col("Engine"))).drop("SubModel")

  val lkq8 = lkq7.withColumn("certification", certificationUDF(col("Certification")))

  var df1 = lkq8.select(col("Partslink"), col("Position"), col("Year"),
    col("Make"), col("Model"), col("Condition").as("condition"), col("ItemLevelGTIN").as("gtin"),
    col("CategoryName"), col("SubCategoryName"), col("ProType"), col("Description").as("description"),
    col("ImageURL"), col("availability"), col("Price").as("price"), col("product_type"),
    col("Brand").as("brand"), col("mpn"), col("PartNumber").as("item_group_id"),
    col("certification"), col("Engine")).distinct

  println("df1 count is " + df1.count())
  println("df1 part count is " + df1.select("mpn").distinct().count())

  df1.write.mode(org.apache.spark.sql.SaveMode.Overwrite).format("csv")
    .option("header","true").option("delimiter","^")
    .save(path + "df1")

}

object Feed {
  def apply(): Feed = new Feed()
}