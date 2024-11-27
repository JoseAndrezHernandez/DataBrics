// Databricks notebook source
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

// COMMAND ----------

val filePath: String = "dbfs:/FileStore/Esophageal_Dataset-1.csv"

// COMMAND ----------

val df = spark.read.option( "header" , "true" ).option("inferSchema" , "true" ).csv(filePath)

// COMMAND ----------

display(
  df
)

// COMMAND ----------

val df_renamed = 
df.withColumnRenamed("patient_barcode", "Codigo_De_Barras_paciente")
.withColumnRenamed("tissue_source_site" , "Sitio de origen de tejido")
.withColumnRenamed("patient_id", "id_paciente")
.withColumnRenamed("bcr_patient_uuid", "rcb_iduu_paciente" )
.withColumnRenamed("informed_consent_verified" , "Consentimiento informado _ verificado")
.withColumnRenamed("tissue_prospective_collection_indicator" , "Indicador_de_recolección_prospectiva_de_tejido.")
.withColumnRenamed("tissue_retrospective_collection_indicator" ,"Indicador_de_recolección_retrospectiva_de_tejido.")
.withColumnRenamed("days_to_birth", "dias_hasta_el_nacimiento")
.withColumnRenamed("country_of_birth" , "pais_nacimiento")
.withColumnRenamed("gender", "genero")
.withColumnRenamed("height","altura")
.withColumnRenamed("weight" , "peso")
.withColumnRenamed("country_of_procurement", "pais_de_adquisicion")
.withColumnRenamed("state_province_of_procurement", "Estado_provincia_de_adqusicion")
.withColumnRenamed("city_of_procurement", "ciudad_de_adquisicion")
.withColumnRenamed("race_list" , "lista_raza")
.withColumnRenamed("ethnicity","Etnicidad")
.withColumnRenamed("other_dx","otro_diagnostico")
.withColumnRenamed("history_of_neoadjuvant_treatment","histria_de_tratamiento_neoayudvante")
.withColumnRenamed("person_neoplasm_cancer_status", "Estado_de_cancer_de_la_neoplasia_de_la_persona")
.withColumnRenamed("vital_status", "estado_vital")
.withColumnRenamed("days_to_last_followup" , "Dias_hasta_el_ultimo_seguimiento")
.withColumnRenamed("days_to_death", "dias_hata_la_muerte")
.withColumnRenamed("tobacco_smoking_history", "historial_de_consumo_de_tabacco")
.withColumnRenamed("age_began_smoking_in_years","edad_en_la_que_empezoAFumar_en_años")
.withColumnRenamed("stopped_smoking_year","año_en_que_dejo_de_fumar")
.withColumnRenamed("number_pack_years_smoked","numero_paquete_años_fumados")
.withColumnRenamed("alcohol_history_documented", "historial_de_alchol_documentado")
.withColumnRenamed("frequency_of_alcohol_consumption" ,"frecuencia_de_consumo_de_alchol")
.withColumnRenamed("amount_of_alcohol_consumption_per_day" , "cantidad_de_alchol_consumido_por_dia")
.withColumnRenamed("reflux_history", "historial_reflujo")
.withColumnRenamed("antireflux_treatment_types" , "tipos_de_tratamientos_para_el_reflujo")
.withColumnRenamed("h_pylori_infection", " infección_por_h_pylori")
.withColumnRenamed("initial_diagnosis_by","diagnóstico_inicial_por")
.withColumnRenamed("barretts_esophagus","esófago_de_Barrett")
.withColumnRenamed("goblet_cells_present","células_caliciformes_presentes")
.withColumnRenamed("history_of_esophageal_cancer", "antecedentes_de_cáncer_de_esófago")
.withColumnRenamed("number_of_relatives_diagnosed","número_de_parientes_diagnosticados")
.withColumnRenamed("has_new_tumor_events_information", "tiene_información_sobre_nuevos_eventos_tumorales")
.withColumnRenamed("day_of_form_completion","día_de_completación_del_formulario")
.withColumnRenamed("month_of_form_completion","mes_de_completación_del_formulario")
.withColumnRenamed("year_of_form_completion","año_de_completación_del_formulario")
.withColumnRenamed("has_follow_ups_information","tiene_información_de_seguimiento")
.withColumnRenamed("has_drugs_information","tiene_información_sobre_medicamentos")

// COMMAND ----------

display(
  df_renamed
)

// COMMAND ----------


  val df_promedio_barret = df_renamed

  .where(
    col("frecuencia_de_consumo_de_alchol").isNotNull && col("historial_de_consumo_de_tabacco").isNotNull && col("esófago_de_Barrett").isNotNull 
  )
  .withColumn(
    "sin_barret"
    ,when(
      col("esófago_de_Barrett")==="No",+1
    )
  )
  .withColumn(
    "con_barret"
    ,when(
      col("esófago_de_Barrett")==="Yes-USA",1
    )
  )
 
display(
  df_promedio_barret
  .groupBy("esófago_de_Barrett","frecuencia_de_consumo_de_alchol","historial_de_consumo_de_tabacco","genero")
    .agg(
      countDistinct("sin_barret").alias("pacientes_sin_barret")
      ,countDistinct("con_barret").alias("pacientes_con_barret")
      ,avg("numero_paquete_años_fumados").alias("promedio_de_paquetes_fumados_en_años")
      ,avg("cantidad_de_alchol_consumido_por_dia").alias("frecuencia_de_consumo_alchol__por_dia")
      )
      .withColumn(
    "frecuencia_alchol"
    ,when(
      col("frecuencia_de_consumo_de_alchol")===7,"Consumo_de_alchol_alto" 
    ).otherwise(when(
       col("frecuencia_de_consumo_de_alchol") between(5,6),"Consumo_de_alcol_moderado" 
    ).otherwise(when(
      col("frecuencia_de_consumo_de_alchol").between(3,4),"Consumo_de_alchol_bajo" 
    ).otherwise(when(
          col("frecuencia_de_consumo_de_alchol").between(0,2),"consumo_poco_o_nulo"
        )
      )
    ))
    )

    .withColumn(
      "frecuencia_tabaco"
      ,when(
        col("historial_de_consumo_de_tabacco")===4,"consumo_alto"
      ).otherwise(when(
        col("historial_de_consumo_de_tabacco").between(2,3),"consumo_diario_moderad"
      ).otherwise(when(
        col("historial_de_consumo_de_tabacco").between(0,1),"fuma_alungas_veces"
      )))
      )
      .where(
        col("promedio_de_paquetes_fumados_en_años").isNotNull && col("frecuencia_de_consumo_alchol__por_dia").isNotNull
      )
      

      
    
)




// COMMAND ----------

  val df_Tasa_mortalidad_residual_tumor = df_renamed
  .where(
    col("primary_pathology_treatment_prior_to_surgery").isNotNull && col("primary_pathology_residual_tumor").isNotNull
  )
  .withColumn(
    "numero_fallecidos"
     ,when(
      col("estado_vital")==="Dead",1
    ).otherwise(0)

  )
  .withColumn(
    "numero_supervivientes"
    ,when(
      col("estado_vital")==="Alive",1
    ).otherwise(0)
  )
    
  display(
   df_Tasa_mortalidad_residual_tumor
    .groupBy("primary_pathology_treatment_prior_to_surgery","primary_pathology_residual_tumor")
  .agg(
    sum("numero_fallecidos").alias("pacientes_fallecidos")
    ,(countDistinct("id_paciente") - sum("numero_fallecidos")).alias("pacientes_supervivientes")
    ,countDistinct("id_paciente").alias("Total_pacientes")
  
    ,avg("numero_fallecidos").alias("Tasa_mortalidad_luego_del_tratamiento")
    ,avg("numero_supervivientes").alias("Tasa_de_supervivencia_luego_del_tratamiento")
    
  )
  )

// COMMAND ----------

display(
 
 df_renamed
  .where(
    col("lista_raza").isNotNull && col("historial_de_consumo_de_tabacco").isNotNull && col("frecuencia_de_consumo_de_alchol").isNotNull 
  )
  .groupBy("primary_pathology_age_at_initial_pathologic_diagnosis","genero","historial_de_consumo_de_tabacco","frecuencia_de_consumo_de_alchol","lista_raza")
  .agg(
    when(
      col("primary_pathology_age_at_initial_pathologic_diagnosis")<21
      ,"Menor_de_edad"
    )
    .otherwise(when(
      col("primary_pathology_age_at_initial_pathologic_diagnosis").between(22,64)
      ,"Adulto"
    ).otherwise(
      when(
      col("primary_pathology_age_at_initial_pathologic_diagnosis")>=65
      ,"Tercera_edad"
    )
    )
     
    ).alias("categoria_edades")
    ,countDistinct("id_paciente").alias("conteo_pacientes")
  
    
    )
    .orderBy("conteo_pacientes")
)

// COMMAND ----------

display(
  df_renamed
  .where(
    col("primary_pathology_lymph_node_metastasis_radiographic_evidence").isNotNull && col("Estado_de_cancer_de_la_neoplasia_de_la_persona").isNotNull
  )
  .groupBy("primary_pathology_lymph_node_metastasis_radiographic_evidence","Estado_de_cancer_de_la_neoplasia_de_la_persona")
  .agg(
    avg("dias_hata_la_muerte").alias("promedio_de_dias_hata_la_muerte")
    ,max("dias_hata_la_muerte").alias("Dias_maximos_vividos")
    ,min("dias_hata_la_muerte").alias("Dias_minimos_vividos")
  )
)

// COMMAND ----------


  val df_mortalidad = df_renamed
  .where(
    col("numero_paquete_años_fumados").isNotNull && col("Estado_de_cancer_de_la_neoplasia_de_la_persona").isNotNull
      )
  
    .withColumn(
      "numero_de_mortalidad"
      ,when(
        col("estado_vital")==="Dead",1
      )
    )
    .withColumn(
      "numero_de_supervivencia"
      ,when(
        col("estado_vital")==="Alive",1
      )
    )
    display(
      df_mortalidad
      .groupBy("numero_paquete_años_fumados","Estado_de_cancer_de_la_neoplasia_de_la_persona")
  .agg(
    (countDistinct("numero_de_mortalidad")/countDistinct("id_paciente")*100).alias("tasa_de_mortalidad_")
    ,(countDistinct("numero_de_supervivencia")/countDistinct("id_paciente")*100).alias("tasa_supervivencia")
    ,countDistinct("id_paciente").("Cantidad_pacientes")

   
  )

    )
    


// COMMAND ----------

display(
  df_renamed
  .groupBy("genero")
  .agg(
    avg("numero_paquete_años_fumados").alias("Promedio_de_paquetes_fumados_por_genero")
    ,countDistinct("id_paciente").alias("Cantidad_pacientes")
  )
  )

// COMMAND ----------


val df_pais = df_renamed
  .where(
    col("pais_nacimiento").isNotNull && col("edad_en_la_que_empezoAFumar_en_años").isNotNull
  )

  .withColumn(
    "Cantidad_Brazil"
    ,when(
      col("pais_nacimiento")==="Brazil",+1
    ).otherwise(0)
  )
.withColumn(
  "Cantidad_Vietnam"
  ,when(
    col("pais_nacimiento")==="Vietnam",+1
  )
)
.withColumn(
  "Cantidad_USA"
  ,when(
    col("pais_nacimiento")==="United States",+1
  )
)
.withColumn(
  "Cantidad_Russia"
  ,when(
    col("pais_nacimiento")==="Russia",+1
  )
)
.withColumn(
  "Cantidad_Ukrania"
  ,when(
    col("pais_nacimiento")==="Ukraine",+1
  )
)
.withColumn(
  "Cantidad_Australia"
  ,when(
    col("pais_nacimiento")==="Australia",+1
  )
)

display(
  df_pais
  .groupBy("id_paciente","edad_en_la_que_empezoAFumar_en_años","estado_vital","pais_nacimiento","genero")
  .agg(

    countDistinct("id_paciente").alias("Pacientes")

    ,avg("Cantidad_Brazil").alias("cantidad_personas_Brazil")

    ,avg("Cantidad_USA").alias("cantidad_personas_USA")

    ,avg("Cantidad_Vietnam").alias("cantidad_personas_Vietnam")

    ,avg("Cantidad_Russia").alias("cantidad_personas_Russia")

    ,avg("Cantidad_Ukrania").alias("cantidad_personas_Ukrania")
    
    ,avg("Cantidad_Australia").alias("cantidad_personas_Australia")

    ,when( 
      col("edad_en_la_que_empezoAFumar_en_años")<21
    ,"MenorDe_Edad"
  ).otherwise("Mayor_de_Edad").alias("Relacion_Menores_y_Mayores_deEdad")
 
  )



)

// COMMAND ----------


  val df_promedio_sobre_nuevos_eventos_tumoraless = df_renamed
  
  .where(
    col("frecuencia_de_consumo_de_alchol").isNotNull && col("edad_en_la_que_empezoAFumar_en_años").isNotNull
  )
  .withColumn(
    "promedio_sobre_nuevos_eventos_tumorales"
    ,when(col("tiene_información_sobre_nuevos_eventos_tumorales") === "NO",1 )
    .otherwise(0)
      
  )
  

  display(
    df_promedio_sobre_nuevos_eventos_tumoraless
  
    .groupBy("frecuencia_de_consumo_de_alchol","edad_en_la_que_empezoAFumar_en_años")
    .agg(
      avg("promedio_sobre_nuevos_eventos_tumorales").alias("promedio_sobre_nuevos_tumores")
      ,countDistinct("id_paciente").alias("Total_pacientes")
    )

  )


// COMMAND ----------

display(
  df_renamed
  .where(
    col("antecedentes_de_cáncer_de_esófago").isNotNull && col("número_de_parientes_diagnosticados").isNotNull && col("histria_de_tratamiento_neoayudvante").isNotNull
  )
  .groupBy("antecedentes_de_cáncer_de_esófago", "número_de_parientes_diagnosticados" ,"tiene_información_sobre_nuevos_eventos_tumorales", "histria_de_tratamiento_neoayudvante","estado_vital")
  .agg(
    countDistinct("id_paciente").alias("total_pacientes")
    
    
  )

    )

// COMMAND ----------

display(
  df_renamed
  .where(

    col("Estado_de_cancer_de_la_neoplasia_de_la_persona").isNotNull && col("dias_hata_la_muerte").isNotNull

  )
  .groupBy("histria_de_tratamiento_neoayudvante","Estado_de_cancer_de_la_neoplasia_de_la_persona")
  .agg(
    countDistinct("id_paciente").alias("total_pacientes")
    ,avg("dias_hata_la_muerte").alias("promedio_dias__hasta_la_muerte")
  )
)
