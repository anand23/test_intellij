package com.ebctech.lkq

import java.net.URLEncoder
import java.util.UUID

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

import scala.collection.mutable.ArrayBuffer

class FeedFunctions extends java.io.Serializable {

  val path = "/home/spineor/Downloads/VARIOUS_FEEDS/PPM/Facebook/"

  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark: SparkSession = SparkSession.builder()
//    .master("spark://prod5:7077")
        .master("local[*]")
    .appName("PPMFeed")
    .getOrCreate


  //FUNCTIONS USED

  /**
   * Function to convert string to array
   */
  val stringToArrayUDF: UserDefinedFunction = udf { input: String => input.split(",") }


  /**
   * UDF for condition
   */
  val conditionUDF = udf { condition: String =>
    if (condition == "Remanufactured" || condition == "remanufactured")
      "refurbished"
    else
      condition
  }


  /**
   *
   * @param input , Function for URL Encoding
   * @return $String
   */
  def URLEnc(input: String): String = {
    URLEncoder.encode(input, "UTF-8")
  }

  val URLEncUDF: UserDefinedFunction = udf(URLEnc(_: String))


  /**
   * Function for encoding null brands
   */
  val nullBrandsUDF: UserDefinedFunction = udf { brand: String =>
    if (brand == "" || brand == null) URLEnc("&")
    else URLEnc(brand)
  }


  /**
   * Function to convert Array to String
   */
  val arrayToStringUDFforLKQ: UserDefinedFunction = udf { input: Seq[String] => input.toSet.mkString(", ") }


  /**
   * Function for generating description
   */
  val descriptionUDF: UserDefinedFunction = udf { (certification: String, Description: String, SubModel: String,
                                                   Engine: String) =>

    if (certification == null || certification == "null" || certification == "")
      Description + ". It fits sub-model(s) (" + SubModel + ") and (" + Engine + ") engine(s)."
    //else if(brand =="Diamond Standard")a= desc + ". It fits sub-model(s) ("+ sub +") and a ("+eng+") engine(s). It is "+ certifi + " certified and " + brand
    else Description + ". It fits sub-model(s) (" + SubModel + ") and a (" + Engine + ") engine(s). It is " +
      certification + " certified."
  }


  /**
   * Function for certification
   */
  val certificationUDF: UserDefinedFunction = udf { certif: String =>
    if (certif == "NSF") "NSF Certified"
    else if (certif == "CAPA") "CAPA Certified"
    else if (certif == "NSF, CAPA") "NSF/CAPA Certified"
    else certif
  }


  /**
   * Function for reducing the length of description
   */
  val reduceLengthUDF: UserDefinedFunction = udf { description: String =>
    if (description.length <= 5000) description
    else description.substring(0, 5000)
  }


  /**
   * Function for availability condition
   */
  val availabilityUDF: UserDefinedFunction = udf { avail: String =>
    if (avail == "Y") "in stock"
    else if (avail == "N") "out of stock"
    else "preorder"
  }


  /**
   * Function to generate random ids
   */
  val idGenUDF: UserDefinedFunction = udf(() => UUID.randomUUID().toString)


  /**
   * Function to concatenate fields
   */
  val fieldsConcatUDF: UserDefinedFunction = udf { input: Map[String, String] =>

    input("categoryName") + "^^" + input("Partslink") + "^^" + input("Position") + "^^" + input("Make") +
      "^^" + input("Model") + "^^" + input("condition") + "^^" + input("gtin") + "^^" + input("description") + "^^" +
      input("ImageURL") + "^^" + input("availability") + "^^" + input("price") + "^^" + input("product_type") + "^^" +
      input("certification") + "^^" + input("brand") + input("mpn") + "^^" + input("item_group_id") + "^^" +
      input("google_product_category") + "^^" + input("ProType") + "^^" + input("Engine")
  }


  /**
   * Function to convert Array to string
   */
  val arrayToStringUDF = udf { input: Seq[String] => input.toSet.toSeq.sorted.mkString(",") }


  /**
   * Function to make year range
   */
  val combineConsecutiveUDF: UserDefinedFunction = udf { input: String =>
    val ints: List[Int] = input.split(',').map(_.toInt).toList.reverse

    ints
      .drop(1)
      .foldLeft(List(List(ints.head)))((acc, e) => if ((acc.head.head - e) <= 1)
        (e :: acc.head) :: acc.tail
      else
        List(e) :: acc)
      .map(group => if (group.size > 1) group.min + "-" + group.max else group.head.toString)
  }


  /**
   * Function to create dynamic Title
   */

  val dynamicTitleUDF: UserDefinedFunction = udf { (Year: String, Make: String, Model: String, Position: String, PartType: String,
                                                    certification: String, brand: String) =>

    if (certification != null && certification != "null" && certification != "") {
      if (Position == "" || Position == "null" || Position == null || Position == "N/A")
        Year + " " + Make + " " + Model + " " + PartType + " (" + certification + ")"
      else
        Year + " " + Make + " " + Model + " " + Position + " " + PartType + " (" + certification + ")"
    }
    else if (brand == "Goodmark") {
      if (Position == "" || Position == "null" || Position == null || Position == "N/A")
        Year + " " + Make + " " + Model + " " + PartType + " (Goodmark)"
      else
        Year + " " + Make + " " + Model + " " + Position + " " + PartType + " (Goodmark)"
    }
    else if (Position == "" || Position == "null" || Position == null || Position == "N/A")
      Year + " " + Make + " " + Model + " " + PartType
    else
      Year + " " + Make + " " + Model + " " + Position + " " + PartType
  }


  val staticTitleGenUDFforLKQ = udf { (brand: String, Position: String, PartType: String, Partslink: String) =>
    if (Position == "" || Position == "null" || Position == null || Position == "N/A")
      brand + " " + PartType + " " + "Partslink Number " + Partslink
    else
      brand + " " + Position + " " + PartType + " " + "Partslink Number " + Partslink
  }


  /**
   * Function to replace dash with underscore
   */
  val dashToUnderscoreUDF: UserDefinedFunction = udf { input: String =>
    input.replaceAll("-", "_")
  }


  /**
   * Function to create sku id
   */
  val skuIdUDF: UserDefinedFunction = udf { (mpn: String, ProType: String, Position: String) =>
    if (Position == "" || Position == null) mpn + "-" + ProType
    else mpn + "-" + ProType + "-" + Position
  }


  val replaceUDF = udf { input: String =>

    input.replaceAll("/", " ").replaceAll(" ", "_")
      .replaceAll("-", "_").replaceAll("___", "_")
      .replaceAll("__", "_").replaceAll(",", "")

  }


  /**
   * Function for null positions
   */
  val nullPositionUDF: UserDefinedFunction = udf { position: String =>
    if (position == "" || position == null || position == "N/A") "/"
    else "/" + URLEnc(position) + "/"
  }


  /**
   * Function to replace space with underscore
   */
  val spaceToUnderscoreUDF: UserDefinedFunction = udf { input: String =>
    input.replaceAll(" ", "_")
  }


  /**
   * fun UDF
   */
  val funUDF: UserDefinedFunction = udf { (title: String, link: String, cl_0: String) =>
    title + "@" + link + "@" + cl_0
  }


  /**
   * Function to create Array
   */
  val createArrayUDF: UserDefinedFunction = udf { (x: String, y: String) =>
    Array(x, y)
  }


  /**
   * Function to separate S and D
   */
  val separateSDUDF: UserDefinedFunction = udf { input: String =>
    if (input == "" || input == null) null
    else input.split("@")
  }


  /**
   * Function to maintain gtin length
   */
  val gtinUDF: UserDefinedFunction = udf { gtin: Long =>
    if (gtin.toString.length > 8 && gtin.toString.length < 12)
      (Array.fill[Long](12 - gtin.toString.length)(0).mkString("") + gtin.toString).toLong
    else if (gtin.toString.length < 8)
      (Array.fill[Long](8 - gtin.toString.length)(0).mkString("") + gtin.toString).toLong
    else gtin
  }


  /**
   * Function for Item group Id Serial
   */
  val itemGroupIdSerialUDF: UserDefinedFunction = udf { (x: String, y: Int) =>

    var i: Int = 1
    var arpart = ArrayBuffer[String]()

    while (i <= y && i <= 9) {
      arpart += x + "_0" + i
      i = i + 1
    }

    while (i <= y && i > 9) {
      arpart += x + "_" + i
      i = i + 1
    }
    arpart
  }


  /**
   * Function for add Arr
   */
  val addArrUDF: UserDefinedFunction = udf { (x: String, y: String, z: Int) =>
    var q: Int = z
    var e: Int = 0
    var addR = ArrayBuffer[String]()
    val idsplit = x.split(",")
    val prtsplit = y.split(",")
    while (q > 0) {
      addR += idsplit(e) + "@" + prtsplit(e)
      q = q - 1
      e = e + 1
    }
    addR
  }


  /**
   * Function for splitting id
   */
  val idSplitUDF: UserDefinedFunction = udf { id: String => id.split("@")(0) }


  /**
   * Function for splitting part
   */
  val partSplitUDF: UserDefinedFunction = udf { part: String => part.split("@")(1) }


  /**
   * Function to create Mobile link
   */
  val mobLinkUDF: UserDefinedFunction = udf { link: String => link.slice(0, 8) + "m." + link.slice(12, link.length) }


  def replaceudff3(category: String, parttype: String, position: String, partnumber: String): String = {
    val cat = category.replaceAll(",", "").replaceAll("/", " ")
      .replaceAll(" ", "_").replaceAll("-", "_")
      .replaceAll("___", "_").replaceAll("__", "_")
    val pt = parttype.replaceAll(",", "").replaceAll("/", " ")
      .replaceAll(" ", "_").replaceAll("-", "_")
      .replaceAll("___", "_").replaceAll("__", "_")
    val pos = position.replaceAll(",", "").replaceAll("/", " ")
      .replaceAll(" ", "_").replaceAll("-", "_")
      .replaceAll("___", "_").replaceAll("__", "_")
    val pn = partnumber.replaceAll(",", "").replaceAll("/", " ")
      .replaceAll(" ", "_").replaceAll("-", "_")
      .replaceAll("___", "_").replaceAll("__", "_")
    val x = cat + "/" + pt + "/" + pos + "/" + pn
    x.toLowerCase()
  }

  val replaceUDF3 = udf(replaceudff3(_: String, _: String, _: String, _: String))

  def replaceudff4(category: String, parttype: String, partnumber: String): String = {
    val cat = category.replaceAll(",", "").replaceAll("/", " ")
      .replaceAll(" ", "_").replaceAll("-", "_")
      .replaceAll("___", "_").replaceAll("__", "_")
    val pt = parttype.replaceAll(",", "").replaceAll("/", " ")
      .replaceAll(" ", "_").replaceAll("-", "_")
      .replaceAll("___", "_").replaceAll("__", "_")
    val pn = partnumber.replaceAll(",", "").replaceAll("/", " ")
      .replaceAll(" ", "_").replaceAll("-", "_")
      .replaceAll("___", "_").replaceAll("__", "_")
    val x = cat + "/" + pt + "/" + pn
    x.toLowerCase()
  }

  val replaceUDF4 = udf(replaceudff4(_: String, _: String, _: String))


  val customLabelUDF = udf { (partType: String, certification: String) =>
    if (partType.contains("Fuel Tank") && certification != "NSF/CAPA Certified") "Fuel Tanks"
    else if (partType.contains("A/C Condenser") && certification != "NSF/CAPA Certified") "A/C Condensers"
    else if (partType.contains("Grille") && certification != "NSF/CAPA Certified") "Grilles"
    else if (partType.contains("Hood") && certification != "NSF/CAPA Certified") "Hood"
    else if (partType.contains("Bumper Cover") && certification != "NSF/CAPA Certified") "Bumper Covers"
    else if (partType.contains("Wheel") && partType != "Wheel Cover" && certification != "NSF/CAPA Certified") "Wheels"
    else if (partType == "Wheel Cover" && certification != "NSF/CAPA Certified") "Wheel Covers"
    else if (partType.contains("Tail Light") && certification != "NSF/CAPA Certified") "Tail Lights"
    else if (partType.contains("Headlight") && certification != "NSF/CAPA Certified") "Headlights"
    else if (partType.contains("Bumper") && !partType.contains("Bumper Cover")
      && certification != "NSF/CAPA Certified") "Bumpers"
    else if (partType.contains("Fender") && certification != "NSF/CAPA Certified") "Fenders"
    else if (partType.contains("Mirror") && certification != "NSF/CAPA Certified") "Mirrors"
    else "Certified"
  }


  val fitmentUDF = udf { fitment: String =>

    val fitSplit = fitment.split("/")

    fitSplit(0) + "@" + fitSplit(1) + "@" + fitSplit(2)

  }


  def Title1(x: String): String = {
    x.replaceAll(";", " ").replaceAll("   ", " ")
      .replaceAll("  ", " ").replaceAll("\\[", "\\(")
      .replaceAll("\\]", "\\)").toLowerCase().capitalize
  }

  val Title1UDF = udf(Title1(_: String))


  /**
   * UDF for making title for Wheel for Feedonomics com.ebctech.lkq.Feed
   */
  val WheelTitleFeedonomicsUDF = udf { (Description: String, Year: String, Make: String, Model: String) =>
    var material = Description.split(";")(0)
    var diameter = Description.split(";")(1)

    if (material.contains("Aluminum") || material.contains("aluminum") || material.contains("Alluminum"))
      material = "Aluminium"
    else
      material = "Steel"

    Year + " " + Make + " " + Model + " " + material + " " + diameter.slice(1, 3) + " inches" + diameter
  }


  /**
   * UDF for making title for Wheel for Google and Bing com.ebctech.lkq.Feed
   */

  val WheelDynamicTitleGoogleAndBingUDF = udf { (Year: String, Make: String, Model: String, Description: String) =>
    val material = Description.split(";")(0)

    if (material.contains("Aluminum") || material.contains("aluminum") || material.contains("Alluminum"))
      Year + " " + Make + " " + Model + " " + "Aluminium" + " Wheel"
    else
      Year + " " + Make + " " + Model + " " + "Steel" + " Wheel"
  }


  /**
   * Function to split Fitment
   */
  val FitmentUDF: UserDefinedFunction = udf { input: String => input.split("|") }


  /**
   * Function to remove static titles from the feed
   */

  import scala.util.{Success, Try}

  val checkIntUDF = udf { (Title: String) =>

    val y = Try(Title.slice(0, 4).toInt)
    y match {
      case Success(x) => "True"
      case _ => "False"
    }
  }


  /**
   * UDF for custom label 1
   */

  val customLabel1UDF = udf { price: String =>
    if (price.toDouble <= 100)
      "<100"
    else if (price.toDouble > 100 && price.toDouble <= 200)
      "<200"
    else
      ">200"
  }

  /**
   * To change the position to driver side or passenger side
   */
  val driverOrPassengerUDF = udf { (description: String, position: String) =>
    val lowerDescription = description.toLowerCase()

    if (lowerDescription.contains("front passenger side"))
      "Front passenger side"
    else if (lowerDescription.contains("rear passenger side"))
      "Rear passenger side"
    else if (lowerDescription.contains("rear driver side"))
      "Rear driver side"
    else if (lowerDescription.contains("front driver side"))
      "Front driver side"
    else if (lowerDescription.contains("driver or passenger side") || lowerDescription.contains("driver side or passenger side"))
      "Driver or passenger side"
    else if (lowerDescription.contains("driver and passenger side"))
      "Driver and passenger side"
    else if (lowerDescription.contains("passenger side"))
      "Passenger side"
    else if (lowerDescription.contains("driver side"))
      "Driver side"
    else if (lowerDescription.contains("driver or passenger side front"))
      "Driver or passenger side front"
    else if (lowerDescription.contains("rear driver or passenger side") || lowerDescription.contains("driver or passenger side rear"))
      "Driver or passenger side rear"
    else
      position
  }

  /**
   * To find the gtin pattern
   */
  val gtinPatternUDF = udf { (gtin: String) =>
    gtin.slice(0, 6)
  }

  /**
   * To convert a field to lower case
   */
  val toLowerUDF = udf { Field: String =>
    Field.toLowerCase()
  }


  /**
   * To make the brand name as manufacturer name as brand name based on condition
   */
  val ManufacturerBrandUDF = udf { (condition: String, MakeName: String, Brand: String) =>
    if (condition == "Refurbished" || condition == "refurbished")
      MakeName
    else
      Brand
  }


  /**
   * 28th Nov, 2019 - Titles based on condition
   */

  val conditionTitleUDF = udf { (condition: String, DynamicTitle: String, mpn: String) =>
    if(condition == "refurbished")
      mpn + " | " + DynamicTitle
    else
      DynamicTitle + " - " + mpn
  }

  val linkLengthUDF = udf {link: String =>
    val arr_link = link.split("/")

    if(arr_link != null)
      arr_link.length
    else
      0
  }

  val title_5thDec2019_null_positionUDF = udf{(mpn: String, oe: String, partType: String) =>
    if(oe == null || oe == "" || oe == "null") {
      mpn + " | " + partType
    }
    else {
      mpn + " | " + oe + " | " + partType
    }
  }


  val title_5thDec2019_not_null_positionUDF = udf{(mpn: String, oe: String, position: String, partType: String) =>

    val pos: Array[String] = position.split("_").map(x => x.capitalize)

    val pos1 = pos.mkString(" ")

    if(oe == null || oe == "" || oe == "null") {
      mpn + " | " + pos1 + " " + partType
    }
    else {
      mpn + " | " + oe + " | " + pos1 + " " + partType
    }
  }

  /**
   * 13th Dec, 2019 - popular sku UDF
   */

  val popularSkuUDF = udf {(custom_label_3:String, popular: String) =>
    if(popular == "popular")
      "popular"
    else
      custom_label_3
  }

  /**
   * 3rd Jan, 2019 - Add pdp in static links
   */

  val DynamicPdpUDF = udf{link: String =>
    val b = link.split("/").takeRight(3)
    val c= link.split("/").drop(3).dropRight(3).mkString("/")
    "https://www.plusmore.com/pdp/"+c+ s"?year=${b(0)}"+s"&make=${b(1)}&"+s"model=${b(2)}"
  }

  /**
   * 3rd Jan, 2019 - Add pdp in static links
   */

  val staticPdpUDF = udf {link: String =>
    if (link != null)
      link.slice(0, 25) + "pdp/" + link.slice(25, link.length)
    else
      link
  }

  //  val titlePipeSpaceUDF = udf {title: String =>
  //    val title1 = title.mkString("").r()
  //  }

}