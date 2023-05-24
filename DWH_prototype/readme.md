#

1. `00_description.md` - описание решения в формате markdown
2. `00_status_graph.png` - иллюстрация к md файлу, схема статусов заказов
3. `02_03_prototype_graph.png` - иллюстрация к md файлу, схема прототипа приемника
4. `01_SRC_cdc_src__DDL_DML.sql` - скрипт для создания сущностей и генератора источника
5. `02_DWH_stage_ods_DDL_DML.sql` - скрипт для создания сущностей и функций слоев stage и ods приемника
6. `03_DWH_report_DDL_DQL.sql` - скрипт для создания сущностей слоя report приемника
7. `04_DWH_test_check_DDL_DML_DQL.sql` - скрипт для создания слоя _test с таблицами логов и проверками данных
8. `05_dag_graph.png` - иллюстрация к md файлу, схема дага Airflow
9. `05_dag.py` - скрипт дага Airflow