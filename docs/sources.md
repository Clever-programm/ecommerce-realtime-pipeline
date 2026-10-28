# Список используемой литературы — рабочий файл

Требование МУ (п. 3.1): **не менее 20 источников**, из них **не менее 3 на английском языке**.
Оформление — ГОСТ Р 7.0.100-2018, ссылки в тексте — ГОСТ Р 7.0.5-2008, вид `[12]` или `[3, с. 112]`.
**На каждый источник в тексте ПЗ должна быть ссылка** — колонка «Где используется» ведётся для самопроверки.

## Как пользоваться файлом

- Нумерация в итоговом списке ПЗ — **в порядке первого упоминания в тексте**, а не по алфавиту. Поэтому финальные номера проставляются в последнюю очередь; здесь ID — рабочие и не меняются.
- Статус: `✅` — выходные данные проверены по источнику; `⚠️` — нужно проверить (год, издательство, число страниц, дату обращения для URL).
- Перед сдачей: все `⚠️` закрыть, все строки с пустой колонкой «Где используется» — либо вставить ссылку в текст, либо убрать из списка.

## Статус

| Всего | Проверено | На английском | Требуется |
|---|---|---|---|
| 28 | 0 | 14 | 20 (в т.ч. 3 англ.) |

Запас сверх минимума сделан намеренно: часть источников отсеется, когда станет ясно, на что реально есть ссылки в тексте.

---

## 1. Книги и монографии

| ID | Источник | № в ПЗ | Яз. | Статус | Где используется |
|---|---|---|---|---|---|
| B1 | Kleppmann, M. Designing Data-Intensive Applications: The Big Ideas Behind Reliable, Scalable, and Maintainable Systems. — Sebastopol: O'Reilly Media, 2017. — 616 p. | **—** | en | ⚠️ | план: 2.1, 3.3 |
| B2 | Клеппман, М. Высоконагруженные приложения. Программирование, масштабирование, поддержка / М. Клеппман. — Санкт-Петербург: Питер, 2018. — 640 с. | **—** | ru | ⚠️ | план: 2.3 |
| B3 | Reis, J. Fundamentals of Data Engineering: Plan and Build Robust Data Systems / J. Reis, M. Housley. — Sebastopol: O'Reilly Media, 2022. — 447 p. | **1** | en | ⚠️ | 1.1 |
| B4 | Akidau, T. Streaming Systems: The What, Where, When, and How of Large-Scale Data Processing / T. Akidau, S. Chernyak, R. Lax. — Sebastopol: O'Reilly Media, 2018. — 352 p. | **3** | en | ⚠️ | 1.2, 1.3 |
| B5 | Chambers, B. Spark: The Definitive Guide: Big Data Processing Made Simple / B. Chambers, M. Zaharia. — Sebastopol: O'Reilly Media, 2018. — 601 p. | **11** | en | ⚠️ | 1.6 |
| B6 | Damji, J. S. Learning Spark: Lightning-Fast Data Analytics / J. S. Damji, B. Wenig, T. Das, D. Lee. — 2nd ed. — Sebastopol: O'Reilly Media, 2020. — 400 p. | **—** | en | ⚠️ | план: 2.3 |
| B7 | Shapira, G. Kafka: The Definitive Guide: Real-Time Data and Stream Processing at Scale / G. Shapira, T. Palino, R. Sivaram, K. Petty. — 2nd ed. — Sebastopol: O'Reilly Media, 2021. — 486 p. | **9** | en | ⚠️ | 1.6 |
| B8 | Kimball, R. The Data Warehouse Toolkit: The Definitive Guide to Dimensional Modeling / R. Kimball, M. Ross. — 3rd ed. — Indianapolis: Wiley, 2013. — 600 p. | **7** | en | ⚠️ | 1.5 |
| B9 | Inmon, W. H. Building the Data Warehouse / W. H. Inmon. — 4th ed. — Indianapolis: Wiley, 2005. — 543 p. | **—** | en | ⚠️ | план: 1.5 (не использован) |
| B10 | DAMA International. DAMA-DMBOK: Data Management Body of Knowledge. — 2nd ed. — Basking Ridge: Technics Publications, 2017. — 590 p. | **2** | en | ⚠️ | 1.1 |
| B11 | Densmore, J. Data Pipelines Pocket Reference: Moving and Processing Data for Analytics / J. Densmore. — Sebastopol: O'Reilly Media, 2021. — 276 p. | **—** | en | ⚠️ | план: 2.2, 2.4 |

> ⚠️ **B2 требует отдельной проверки**: у издательства «Питер» выходило несколько переизданий с разной пагинацией. Сверить по карточке книги, а не по памяти.
> ⚠️ Русскоязычных источников в списке мало. Руководитель может потребовать наличие изданий из IPR SMART / eLIBRARY (в МУ в качестве образца приведён именно источник IPRbooks). Это нужно уточнить при согласовании задания — подобрать такие источники по каталогу, а не наугад.

## 2. Научные статьи и доклады

| ID | Источник | № в ПЗ | Яз. | Статус | Где используется |
|---|---|---|---|---|---|
| A1 | Kreps, J. Kafka: a Distributed Messaging System for Log Processing / J. Kreps, N. Narkhede, J. Rao // Proceedings of the NetDB Workshop. — Athens, 2011. — P. 1–7. | **8** | en | ⚠️ | 1.6 |
| A2 | Zaharia, M. Discretized Streams: Fault-Tolerant Streaming Computation at Scale / M. Zaharia, T. Das, H. Li [et al.] // Proceedings of the 24th ACM Symposium on Operating Systems Principles (SOSP'13). — New York: ACM, 2013. — P. 423–438. | **—** | en | ⚠️ | план: 2.3 |
| A3 | Armbrust, M. Structured Streaming: A Declarative API for Real-Time Applications in Apache Spark / M. Armbrust, T. Das, J. Torres [et al.] // Proceedings of the 2018 International Conference on Management of Data (SIGMOD'18). — New York: ACM, 2018. — P. 601–613. | **10** | en | ⚠️ | 1.6 |
| A4 | Carbone, P. Apache Flink: Stream and Batch Processing in a Single Engine / P. Carbone, A. Katsifodimos, S. Ewen [et al.] // Bulletin of the IEEE Computer Society Technical Committee on Data Engineering. — 2015. — Vol. 38, № 4. — P. 28–38. | **12** | en | ⚠️ | 1.6 |

## 3. Стандарты и нормативные документы

| ID | Источник | № в ПЗ | Яз. | Статус | Где используется |
|---|---|---|---|---|---|
| S1 | ГОСТ 34.602-89. Информационная технология. Комплекс стандартов на автоматизированные системы. Техническое задание на создание автоматизированной системы. — Москва: Стандартинформ, 2009. — 12 с. | **5** | ru | ⚠️ | 1.4 |
| S2 | ГОСТ 19.201-78. Единая система программной документации. Техническое задание. Требования к содержанию и оформлению. — Москва: Стандартинформ, 2010. — 3 с. | **4** | ru | ⚠️ | 1.4 |
| S3 | ISO/IEC/IEEE 29148:2018. Systems and software engineering — Life cycle processes — Requirements engineering. — Geneva: ISO, 2018. — 104 p. | **6** | en | ⚠️ | 1.4 |
| S4 | ГОСТ Р ИСО/МЭК 25010-2015. Информационные технологии. Системная и программная инженерия. Требования и оценка качества систем и программного обеспечения (SQuaRE). Модели качества систем и программных продуктов. — Москва: Стандартинформ, 2015. | **—** | ru | ⚠️ | план: 3.3 |
| S5 | ГОСТ Р 7.0.100-2018. Система стандартов по информации, библиотечному и издательскому делу. Библиографическая запись. Библиографическое описание. Общие требования и правила составления. — Москва: Стандартинформ, 2018. | **—** | ru | ⚠️ | план: оформление списка |
| S6 | ГОСТ Р 7.0.5-2008. Система стандартов по информации, библиотечному и издательскому делу. Библиографическая ссылка. Общие требования и правила составления. — Москва: Стандартинформ, 2008. | **—** | ru | ⚠️ | план: оформление ссылок |

## 4. Электронные ресурсы и документация

Для всех записей этого раздела обязательны `— URL: ... (дата обращения: ДД.ММ.2026)` и пометка `— Текст: электронный`.

| ID | Источник | № в ПЗ | Яз. | Статус | Где используется |
|---|---|---|---|---|---|
| E1 | Apache Kafka Documentation: [сайт]. — URL: https://kafka.apache.org/documentation/ (дата обращения: __.__.2026). — Текст: электронный. | **—** | en | ⚠️ | план: 2.1, 2.2 |
| E2 | Structured Streaming Programming Guide // Apache Spark: [сайт]. — URL: https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html (дата обращения: __.__.2026). — Текст: электронный. | **—** | en | ⚠️ | план: 2.3 |
| E3 | Apache Airflow Documentation: [сайт]. — URL: https://airflow.apache.org/docs/ (дата обращения: __.__.2026). — Текст: электронный. | **13** | en | ⚠️ | 1.6 |
| E4 | PostgreSQL 15 Documentation // The PostgreSQL Global Development Group: [сайт]. — URL: https://www.postgresql.org/docs/15/ (дата обращения: __.__.2026). — Текст: электронный. | **15** | en | ⚠️ | 1.6 |
| E5 | ClickHouse Documentation: [сайт]. — URL: https://clickhouse.com/docs (дата обращения: __.__.2026). — Текст: электронный. | **16** | en | ⚠️ | 1.6 |
| E6 | MinIO Object Storage Documentation: [сайт]. — URL: https://min.io/docs/minio/linux/index.html (дата обращения: __.__.2026). — Текст: электронный. | **14** | en | ⚠️ | 1.6 |
| E7 | Great Expectations Documentation: [сайт]. — URL: https://docs.greatexpectations.io/ (дата обращения: __.__.2026). — Текст: электронный. | **17** | en | ⚠️ | 1.6 |
| E8 | Prometheus Documentation: [сайт]. — URL: https://prometheus.io/docs/ (дата обращения: __.__.2026). — Текст: электронный. | **18** | en | ⚠️ | 1.6 |
| E9 | Grafana Documentation // Grafana Labs: [сайт]. — URL: https://grafana.com/docs/grafana/latest/ (дата обращения: __.__.2026). — Текст: электронный. | **—** | en | ⚠️ | план: 3.4 |
| E10 | Docker Compose Documentation // Docker Inc.: [сайт]. — URL: https://docs.docker.com/compose/ (дата обращения: __.__.2026). — Текст: электронный. | **—** | en | ⚠️ | план: 2.1 |
| E11 | pytest Documentation: [сайт]. — URL: https://docs.pytest.org/ (дата обращения: __.__.2026). — Текст: электронный. | **—** | en | ⚠️ | план: 3.1, 3.2 |

---

## Нумерация

Номера **[1]–[18]** закреплены по порядку первого упоминания в главе 1 и
проставлены в тексте главы. Перечень этих 18 источников продублирован в конце
файла главы — отдельным разделом, чтобы при проверке было видно, на что ведёт
каждая ссылка. В окончательной пояснительной записке этот раздел войдёт в
общий «Список используемой литературы».

Нумерация сплошная, без пропусков: источники, на которые в главе 1 ссылок нет,
номера не получили. Они пронумеруются при первом упоминании в главах 2–3,
начиная с 19 — это B1, B2, B6, B9, B11, A2, S4, S5, S6, E1, E2, E9, E10, E11.

Минимум по МУ — 20 источников, из них 3 на английском. После главы 1 набрано
18 (все с ссылками), так что порог закроется уже в главе 2.

## Что осталось сделать

- [ ] Проверить выходные данные всех записей со статусом ⚠️ (издательство, год, число страниц).
- [ ] Согласовать с руководителем долю русскоязычных источников и необходимость изданий из IPR SMART / eLIBRARY.
- [ ] Проставить даты обращения для всех электронных ресурсов — одной датой, ближе к сдаче.
- [ ] После готовности текста ПЗ — перенумеровать список в порядке первого упоминания и сверить, что на каждый источник есть ссылка.
