from pyspark.sql import SparkSession, functions as f
from pyspark.sql.types import StructType,StructField,StringType,MapType,ArrayType,LongType, DateType

spark = SparkSession.builder.appName("Book Schemas").getOrCreate()

editionSchema = StructType([
        StructField("key",StringType(),False),
        StructField("title",StringType(),False),
        StructField("subtitle",StringType(),True),
        StructField("type",StructType([
                StructField("key",StringType(),False)
            ])),
        StructField("authors",ArrayType(StructType([StructField("key",StringType(),False)]),True)),
        StructField("works",ArrayType(StructType([StructField("key",StringType(),False)]),False)),
        StructField("identifiers",MapType(StringType(),StringType()),True),
        StructField("isbn_10",ArrayType(StringType()),True),
        StructField("isbn_13",ArrayType(StringType()),True),
        StructField("lccn",ArrayType(StringType()),True),
        StructField("ocaid",ArrayType(StringType()),True),
        StructField("oclc_numbers",ArrayType(StringType()),True),
        StructField("covers",ArrayType(LongType()),True),
        StructField("links",ArrayType(StructType([
                StructField("url",StringType(),False),
                StructField("title",StringType(),False),
                StructField("type",StructType([StructField("key",StringType(),False)]))
            ])),True),
        StructField("languages",ArrayType(StructType([StructField("key",StringType(),True)])),True),
        StructField("by_statement",StringType(),True),
        StructField("weight",StringType(),True),
        StructField("edition_name",StringType(),True),
        StructField("number_of_pages",LongType(),True),
        StructField("pagination",StringType(),True),
        StructField("physical_dimensions",StringType(),True),
        StructField("physical_format",StringType(),True),
        StructField("publish_country",StringType(),True),
        StructField("publish_date",StringType(),True),
        StructField("publish_places",ArrayType(StringType()),True),
        StructField("publishers",ArrayType(StringType()),True),
        StructField("contributions",ArrayType(StringType()),True),
        StructField("dewey_decimal_class",ArrayType(StringType()),True),
        StructField("genres",ArrayType(StringType()),True),
        StructField("lc_classifications",ArrayType(StringType()),True),
        StructField("other_titles",ArrayType(StringType()),True),
        StructField("series",ArrayType(StringType()),True),
        StructField("source_records",ArrayType(StringType()),True),
        StructField("subjects",ArrayType(StringType()),True),
        StructField("work_titles",ArrayType(StringType()),True),
        StructField("table_of_contents",ArrayType(StringType()),True),
        StructField("description",StructType([
                StructField("value",StringType(),False),
            ]),True),
        StructField("first_sentence",StructType([
                StructField("value",StringType(),False),
            ]),True),
        StructField("notes",StructType([
                StructField("value",StringType(),False),
            ]),True),
        StructField("revision",LongType(),True),
        StructField("latest_revision",LongType(),True),
        StructField("created",StructType([
               StructField("value",DateType(),False),
           ]),True),
        StructField("last_modified",StructType([
                StructField("value",DateType(),False),
            ]),True)
    ])

authorSchema = StructType([
        StructField("key",StringType(),False),
        StructField("name",StringType(),False),
        StructField("type",StructType([
                StructField("key",StringType(),False)
            ])),
        StructField("alternate_names",ArrayType(StringType()),True),
        StructField("bio",StructType([
                StructField("key",StringType(),False),
                StructField("value",StringType(),False)
            ]),True),
        StructField("birth_date",StringType(),True),
        StructField("death_date",StringType(),True),
        StructField("location",StringType(),True),
        StructField("date",StringType(),True),
        StructField("entity_type",StringType(),True),
        StructField("fuller_name",StringType(),True),
        StructField("personal_name",StringType(),True),
        StructField("title",StringType(),True),
        StructField("photos",ArrayType(LongType()),True),
        StructField("links",ArrayType(StructType([
                StructField("url",StringType(),False),
                StructField("title",StringType(),False),
                StructField("type",StructType([
                        StructField("key",StringType(),False),
                    ]),True),
            ]),True)),
        StructField("remote_ids",StructType([
                StructField("wikidata",StringType(),False),
                StructField("viaf",StringType(),False),
            ]),True),
        StructField("revision",LongType(),True),
        StructField("latest_revision",LongType(),True),
        StructField("created",StructType([
                StructField("value",DateType(),False),
            ]),True),
        StructField("last_modified",StructType([
                StructField("value",DateType(),False),
            ]),True)
    ])
