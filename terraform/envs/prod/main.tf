module "postgres" {
  source                    = "../../modules/postgres"
  warehouse_db_name         = "banking_dw"
  etl_writer_password       = var.etl_writer_password
  streamlit_reader_password = var.streamlit_reader_password
  grafana_reader_password   = var.grafana_reader_password
}

module "object_storage" {
  source      = "../../modules/object_storage"
  bucket_name = "banking-lakehouse"
  env         = "prod"
}
