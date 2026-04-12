<!-- 4ccbbb97-50cf-4dba-b49d-eda86021b9e0 199705ea-106e-488e-aae6-66a608309323 -->
# COW Const-Correctness Remediation - Per-file TODOs

### Approach

- Apply the guide’s mechanical rules: read-only uses `const Column::Ptr&`, write uses `Column::MutablePtr` and `*_mutable_ptr()` accessors; internal subcolumns store `WrappedPtr` in `*_column.h` files.
- Fix signatures first, then calling sites; avoid `const_cast` and raw pointer writes.

### Implementation TODOS

- Each item: atomic, file-scoped, action-led, aligned with guide.

1. bench/bench_util.h — audit helpers: const-read vs write; update signatures to `const Ptr&`/`MutablePtr`.
2. bench/cow_bench.cpp — ensure benchmarks use `MutablePtr` for writes; replace `.get()` raw writes with `try_mutate()` or `*_mutable_ptr()`.
3. bench/hash_functions_bench.cpp — fix any write paths to columns using `MutablePtr`; read paths as `const Ptr&`.
4. column/chunk.cpp — ensure mutating paths use `get_mutable_column_by_*`; internal updates avoid writing via `ColumnPtr`.
5. column/chunk_extra_data.cpp — segregate read/write; use `MutablePtr` for stored mutables.
6. column/column.cpp — audit all methods: read-only const, write via `MutablePtr`; remove unsafe `Column*` writes.
7. column/column_helper.cpp — add/verify `as_raw_column(Column*)` overload usage; avoid const-casts; prefer `mutable_data_column()`.
8. column/column_helper.h — ensure helper templates accept `MutablePtr` and const-correct overloads; no leaking `WrappedPtr`.
9. column/column_view/column_view_helper.cpp — read-only view helpers: enforce const; no mutation through views.
10. column/column_viewer.cpp — ensure viewer is read-only; remove any mutating calls.
11. column/const_column.cpp — guarantee mutations apply to underlying via proper APIs; avoid raw pointer writes.
12. column/const_column.h — expose `data_column_ptr()`/const access; no mutating accessors.
13. column/json_column.cpp — write paths via `MutablePtr`; subcolumns use `*_mutable_ptr()`.
14. connector/binlog_connector.cpp — filling columns uses `get_mutable_column_by_*`; Nullable uses `mutable_data_column()`.
15. exec/agg_runtime_filter_builder.cpp — destination columns as `MutablePtr`; fix callsites.
16. exec/aggregate/agg_hash_map.h — interfaces expecting output columns accept `MutablePtr`; inputs `const Ptr&`.
17. exec/aggregate/agg_hash_set.h — same as map.h; no mutation via const.
18. exec/aggregate/compress_serializer.cpp — serialization writes via `MutablePtr`; read buffers const.
19. exec/aggregator.cpp — fix streaming/filter paths: use `try_mutate()`; resolve Columns vs MutableColumns binding.
20. exec/analytor.cpp — outputs as `MutablePtr`; const inputs; update invocations.
21. exec/arrow_to_starrocks_converter.cpp — destination columns `MutablePtr`; subcolumns via `*_mutable_ptr()`.
22. exec/chunks_sorter_full_sort.cpp — ensure in-place writes use `MutablePtr`; read paths const.
23. exec/cross_join_node.cpp — build output columns with `MutablePtr`; no writes on `ColumnPtr`.
24. exec/es/es_scroll_parser.cpp — parsing appends via `MutablePtr`; const-read JSON.
25. exec/except_hash_set.cpp — destination writes `MutablePtr`; inputs const.
26. exec/except_node.cpp — chunk mutation via `get_mutable_column_by_*`.
27. exec/exchange_node.cpp — mutable outputs; const inputs.
28. exec/exec_node.cpp — unify helpers: fill functions accept `MutablePtr`.
29. exec/file_scanner/avro_scanner.cpp — append paths use `MutablePtr`; offsets/elements via `*_mutable_ptr()`.
30. exec/file_scanner/json_scanner.cpp — column fills via `get_mutable_column_by_index`; Nullable `mutable_data_column()`.
31. exec/hash_join_node.cpp — build/probe writes via `MutablePtr`; prevent writes via `ColumnPtr`.
32. exec/hash_joiner.h — method signatures: mutable outputs; const inputs; adjust containers to `MutableColumns`.
33. exec/hdfs_scanner/hdfs_scanner_orc.cpp — similar to parquet/csv fixes: use `*_mutable_ptr()`.
34. exec/intersect_hash_set.cpp — output columns `MutablePtr`; inputs const.
35. exec/intersect_node.cpp — same as except_node: `get_mutable_column_by_*`.
36. exec/jdbc_scanner.cpp — fill chunk via `get_mutable_column_by_slot_id`.
37. exec/join/join_hash_map.cpp — ensure insert/write APIs take `MutablePtr`; reads const.
38. exec/join/join_hash_map.h — adjust interface types; avoid raw `Column*` from const.
39. exec/join/join_key_constructor.hpp — read-only utilities be const-correct.
40. exec/partition/partition_hash_map.h — write params `MutablePtr`; reads const.
41. exec/pipeline/dict_decode_operator.cpp — output columns `MutablePtr`; containers `MutableColumns`.
42. exec/pipeline/exchange/exchange_merge_sort_source_operator.cpp — mutable outputs in push/pop; no write via const.
43. exec/pipeline/nljoin/nljoin_probe_operator.cpp — probe outputs `MutablePtr`.
44. exec/pipeline/nljoin/spillable_nljoin_probe_operator.cpp — same as above.
45. exec/pipeline/project_operator.cpp — destination chunk columns via `get_mutable_column_by_*`.
46. exec/pipeline/scan/schema_chunk_source.cpp — scanner outputs `MutablePtr`; chunk `get_mutable_column_by_*`.
47. exec/pipeline/select_operator.cpp — write to output with `MutablePtr`.
48. exec/pipeline/set/except_context.cpp — mutable outputs; reads const.
49. exec/pipeline/set/intersect_context.cpp — same adjustment.
50. exec/pipeline/table_function_operator.cpp — function outputs `MutablePtr`.
51. exec/schema_scan_node.cpp — all fill helpers accept `MutablePtr`; callers use `get_mutable_column_by_*`.
52. exec/schema_scanner/schema_applicable_roles_scanner.cpp — use chunk mutable column getters; helpers accept `MutablePtr`.
53. exec/schema_scanner/schema_be_bvars_scanner.cpp — same as above.
54. exec/schema_scanner/schema_be_cloud_native_compactions_scanner.cpp — same.
55. exec/schema_scanner/schema_be_compactions_scanner.cpp — same.
56. exec/schema_scanner/schema_be_configs_scanner.cpp — same.
57. exec/schema_scanner/schema_be_datacache_metrics_scanner.cpp — same.
58. exec/schema_scanner/schema_be_logs_scanner.cpp — same.
59. exec/schema_scanner/schema_be_metrics_scanner.cpp — same.
60. exec/schema_scanner/schema_be_tablets_scanner.cpp — same.
61. exec/schema_scanner/schema_be_threads_scanner.cpp — same.
62. exec/schema_scanner/schema_be_txns_scanner.cpp — same.
63. exec/schema_scanner/schema_charsets_scanner.cpp — same.
64. exec/schema_scanner/schema_collations_scanner.cpp — same.
65. exec/schema_scanner/schema_columns_scanner.cpp — same.
66. exec/schema_scanner/schema_fe_metrics_scanner.cpp — same.
67. exec/schema_scanner/schema_fe_tablet_schedules_scanner.cpp — same.
68. exec/schema_scanner/schema_keywords_scanner.cpp — same.
69. exec/schema_scanner/schema_load_tracking_logs_scanner.cpp — same.
70. exec/schema_scanner/schema_materialized_views_scanner.cpp — same.
71. exec/schema_scanner/schema_recyclebin_catalogs.cpp — same.
72. exec/schema_scanner/schema_schema_privileges_scanner.cpp — same.
73. exec/schema_scanner/schema_schemata_scanner.cpp — same.
74. exec/schema_scanner/schema_table_privileges_scanner.cpp — same.
75. exec/schema_scanner/schema_tables_config_scanner.cpp — same.
76. exec/schema_scanner/schema_tables_scanner.cpp — same.
77. exec/schema_scanner/schema_user_privileges_scanner.cpp — same.
78. exec/schema_scanner/schema_variables_scanner.cpp — same.
79. exec/schema_scanner/schema_views_scanner.cpp — same.
80. exec/schema_scanner/starrocks_grants_to_scanner.cpp — same.
81. exec/schema_scanner/starrocks_role_edges_scanner.cpp — same.
82. exec/short_circuit_hybrid.cpp — enforce mutable writes.
83. exec/sorted_streaming_aggregator.cpp — outputs via `MutablePtr`.
84. exec/sorting/merge_path.cpp — ensure no mutation through const views.
85. exec/sorting/sort_column.cpp — write on `MutablePtr`.
86. exec/sorting/sort_permute.cpp — mutation paths via `MutablePtr`.
87. exec/stream/aggregate/stream_aggregator.cpp — chunk writes with `get_mutable_column_by_*`.
88. exec/stream/state/mem_state_table.cpp — state column writes on `MutablePtr`.
89. exec/table_function_node.cpp — function outputs `MutablePtr`.
90. exec/tablet_sink.cpp — sink writes through `MutablePtr`.
91. exprs/agg/array_agg.h — outputs `MutablePtr`; subcolumns via mutable accessors.
92. exprs/agg/array_union_agg.h — same.
93. exprs/agg/combinator/state_merge_function.h — destination `MutablePtr`.
94. exprs/agg/combinator/state_union_function.h — destination `MutablePtr`.
95. exprs/agg/factory/aggregate_resolver_avg.cpp — pass mutable outputs; const inputs.
96. exprs/agg/factory/aggregate_resolver_minmaxany.cpp — same.
97. exprs/agg/factory/aggregate_resolver_others.cpp — same.
98. exprs/agg/factory/aggregate_resolver_sumcount.cpp — same.
99. exprs/agg/java_udaf_function.h — ensure serialize/deserialize use `MutablePtr`.
100. exprs/agg/map_agg.h — subcolumns via `*_mutable_ptr()`.
101. exprs/agg/maxmin_by.h — outputs mutable; inputs const.
102. exprs/agg/window_funnel.h — same.
103. exprs/array_element_expr.cpp — enforce read-only vs write separation.
104. exprs/array_map_expr.cpp — outputs mutate via `MutablePtr`.
105. exprs/binary_function.h — const-correct signatures; mutable outputs.
106. exprs/binary_functions.cpp — fix any writes on `ColumnPtr`.
107. exprs/case_expr.cpp — mutable result columns.
108. exprs/cast_expr.cpp — ensure write paths use `MutablePtr`.
109. exprs/cast_nested.cpp — subcolumns via mutable accessors.
110. exprs/condition_expr.cpp — outputs `MutablePtr`.
111. exprs/dictionary_get_expr.cpp — outputs via `MutablePtr`.
112. exprs/expr_context.cpp — result columns writable; inputs const.
113. exprs/function_call_expr.cpp — destination `MutablePtr`.
114. exprs/function_helper.cpp — unify helpers: write params `MutablePtr`.
115. exprs/gin_functions.cpp — use mutable for writes; const for reads.
116. exprs/hash_functions.cpp — mutable outputs; immutable inputs.
117. exprs/in_const_predicate.hpp — const-only utilities.
118. exprs/info_func.cpp — outputs via mutable.
119. exprs/literal.cpp — creation returns `MutablePtr` if caller writes.
120. exprs/locate.cpp — output column writes via `MutablePtr`.
121. exprs/map_apply_expr.cpp — subcolumns mutable for writes.
122. exprs/math_functions.cpp — outputs mutable; inputs const.
123. exprs/str_to_map.cpp — mutable outputs.
124. exprs/string_functions.cpp — write paths corrected.
125. exprs/struct_functions.cpp — mutable subcolumns accessors.
126. exprs/table_function/json_each.cpp — outputs mutable; subcolumns mutable.
127. exprs/table_function/list_rowsets.cpp — mutable outputs.
128. exprs/table_function/multi_unnest.h — internal mutable; external const/mutable APIs.
129. exprs/table_function/unnest.h — same pattern.
130. exprs/time_functions.cpp — outputs mutable.
131. formats/avro/cpp/complex_column_reader.cpp — mutable dst; subcolumns mutable.
132. formats/avro/cpp/nullable_column_reader.cpp — same.
133. formats/avro/nullable_column.cpp — writes via `MutablePtr`; subcolumns mutable.
134. formats/csv/array_converter.cpp — use `*_mutable_ptr()` for offsets/elements.
135. formats/csv/map_converter.cpp — mutable keys/values/offsets.
136. formats/csv/nullable_converter.cpp — nullable subcolumns mutable.
137. formats/json/map_column.cpp — mutable subcolumns; avoid const writes.
138. formats/json/nullable_column.cpp — mutable data/null columns.
139. formats/json/struct_column.cpp — fields via `fields_column_mutable()`.
140. runtime/data_stream_sender.cpp — output chunk columns via `get_mutable_column_by_*`.
141. runtime/global_dict/miscs.cpp — decode writes via mutable outputs.
142. storage/binlog_reader.cpp — appends via mutable columns.
143. storage/chunk_helper.cpp — creators return `MutablePtr` when caller writes.
144. storage/column_aggregate_func.cpp — aggregation writes use mutable.
145. storage/column_aggregator.h — members that write store `MutablePtr`.
146. storage/convert_helper.cpp — target columns `MutablePtr`.
147. storage/delta_writer.cpp — sink write paths use mutable.
148. storage/index/vector/vector_index_writer.cpp — target columns mutable.
149. storage/lake/delta_writer.cpp — same.
150. storage/local_tablet_reader.cpp — result columns mutable.
151. storage/memtable.cpp — internal mutable members for writes.
152. storage/meta_reader.cpp — output columns mutable.
153. storage/push_utils.cpp — writes via mutable columns.
154. storage/rowset/array_column_iterator.cpp — subcolumns mutable for fills.
155. storage/rowset/binary_dict_page.cpp — destination mutable; inputs const.
156. storage/rowset/cast_column_iterator.cpp — dst mutable; src const.
157. storage/rowset/column_iterator.cpp — fill paths mutable.
158. storage/rowset/column_writer.cpp — internal buffers mutable; outputs mutable.
159. storage/rowset/dict_page.cpp — mutable destination.
160. storage/rowset/dictcode_column_iterator.cpp — dst mutable; src const.
161. storage/rowset/dictcode_column_iterator.h — interface updates for mutable dst.
162. storage/rowset/json_column_compactor.cpp — mutable subcolumns.
163. storage/rowset/json_column_iterator.cpp — dst mutable; src const.
164. storage/rowset/json_column_writer.cpp — writing via mutable.
165. storage/rowset/map_column_iterator.cpp — subcolumns mutable.
166. storage/rowset/parsed_page.cpp — mutable destination buffers.
167. storage/rowset/segment_iterator.cpp — materialize via mutable columns.
168. storage/rowset/struct_column_iterator.cpp — fields mutable.
169. storage/rowset/zone_map_index.cpp — build paths use mutable.
170. storage/rowset_column_update_state.cpp — state columns mutable.
171. storage/rowset_merger.cpp — merge into mutable outputs; inputs const.
172. storage/schema_change_utils.cpp — target columns mutable; src const.
173. storage/tablet_reader.cpp — materialize into mutable columns.
174. udf/java/java_data_converter.cpp — conversion writes via mutable dst.
175. udf/udf_call_stub.cpp — dst mutable; src const.
176. util/json_flattener.cpp — all write paths via mutable subcolumns.

