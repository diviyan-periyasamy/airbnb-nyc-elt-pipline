const fs = require("fs");
const {
  Document, Packer, Paragraph, TextRun, HeadingLevel, Table, TableRow, TableCell,
  WidthType, ShadingType, ImageRun, AlignmentType, BorderStyle, PageBreak,
  LevelFormat, convertInchesToTwip,
} = require("docx");

const img = (name) => fs.readFileSync(`diagrams/${name}`);

// ---- helpers ---------------------------------------------------------------
function h1(text) {
  return new Paragraph({ text, heading: HeadingLevel.HEADING_1, spacing: { before: 300, after: 150 } });
}
function h2(text) {
  return new Paragraph({ text, heading: HeadingLevel.HEADING_2, spacing: { before: 200, after: 100 } });
}
function p(text, opts = {}) {
  return new Paragraph({ children: [new TextRun({ text, ...opts })], spacing: { after: 120 } });
}
function bullet(text, opts = {}) {
  return new Paragraph({
    children: [new TextRun({ text, ...opts })],
    numbering: { reference: "bullets", level: 0 },
    spacing: { after: 60 },
  });
}
function code(lines) {
  return new Paragraph({
    children: [new TextRun({ text: lines, font: "Consolas", size: 18 })],
    shading: { type: ShadingType.CLEAR, fill: "F2F2F2" },
    spacing: { before: 100, after: 200 },
    border: {
      top: { style: BorderStyle.SINGLE, size: 2, color: "CCCCCC" },
      bottom: { style: BorderStyle.SINGLE, size: 2, color: "CCCCCC" },
      left: { style: BorderStyle.SINGLE, size: 2, color: "CCCCCC" },
      right: { style: BorderStyle.SINGLE, size: 2, color: "CCCCCC" },
    },
  });
}
function imgPara(name, width, height, maxWidth = 620) {
  const scale = Math.min(1, maxWidth / width);
  return new Paragraph({
    children: [new ImageRun({ type: "png", data: img(name), transformation: { width: width * scale, height: height * scale } })],
    alignment: AlignmentType.CENTER,
    spacing: { before: 100, after: 100 },
  });
}
function caption(text) {
  return new Paragraph({
    children: [new TextRun({ text, italics: true, size: 20, color: "555555" })],
    alignment: AlignmentType.CENTER,
    spacing: { after: 240 },
  });
}
function cell(text, opts = {}) {
  return new TableCell({
    width: { size: opts.width || 2000, type: WidthType.DXA },
    shading: opts.header ? { type: ShadingType.CLEAR, fill: "2B6CB0" } : undefined,
    children: [new Paragraph({ children: [new TextRun({ text, bold: !!opts.header, color: opts.header ? "FFFFFF" : "000000", size: 20 })] })],
  });
}
function simpleTable(headers, rows, widths) {
  const colWidths = widths || headers.map(() => Math.floor(9000 / headers.length));
  return new Table({
    width: { size: 9000, type: WidthType.DXA },
    columnWidths: colWidths,
    rows: [
      new TableRow({ children: headers.map((hh, i) => cell(hh, { header: true, width: colWidths[i] })) }),
      ...rows.map((r) => new TableRow({ children: r.map((c, i) => cell(String(c), { width: colWidths[i] })) })),
    ],
  });
}

// ---- document ---------------------------------------------------------------
const doc = new Document({
  numbering: {
    config: [{ reference: "bullets", levels: [{ level: 0, format: LevelFormat.BULLET, text: "•", alignment: AlignmentType.LEFT, style: { paragraph: { indent: { left: 420, hanging: 260 } } } }] }],
  },
  sections: [{
    properties: { page: { size: { width: 12240, height: 15840 }, margin: { top: 1080, bottom: 1080, left: 1080, right: 1080 } } },
    children: [
      // ---- Title page ----
      new Paragraph({ spacing: { before: 1200 }, children: [] }),
      new Paragraph({
        alignment: AlignmentType.CENTER,
        children: [new TextRun({ text: "Data Engineering — Coursework 1", bold: true, size: 44, color: "2B6CB0" })],
        spacing: { after: 200 },
      }),
      new Paragraph({
        alignment: AlignmentType.CENTER,
        children: [new TextRun({ text: "Technical Report", bold: true, size: 32 })],
        spacing: { after: 100 },
      }),
      new Paragraph({
        alignment: AlignmentType.CENTER,
        children: [new TextRun({ text: "A Normalized ELT Pipeline for the NYC Airbnb Open Data (2019), Orchestrated with Apache Airflow", size: 24, italics: true, color: "555555" })],
        spacing: { after: 400 },
      }),
      new Paragraph({ children: [new PageBreak()] }),

      // ---- 1. Problem Statement & Dataset ----
      h1("1. Problem Statement and Dataset"),
      p("Modern organisations generate operational data far faster than any single person can process it by hand. The purpose of this coursework is to design and implement a small but complete data engineering pipeline that demonstrates the core competencies covered in the module: ingesting a real-world dataset, cleaning and validating it, transforming it into an analysis-ready form, loading it into a normalized relational schema, and orchestrating the whole process automatically with Apache Airflow, rather than relying on a person to run each step manually every day."),
      h2("1.1 Dataset"),
      p("The pipeline uses the NYC Airbnb Open Data (2019) dataset, a public dataset describing every active Airbnb listing in New York City (originally distributed via Kaggle / Inside Airbnb). It contains 48,895 listing records across 16 columns, comfortably exceeding the coursework's 20,000-record minimum."),
      simpleTable(
        ["Column", "Description"],
        [
          ["id, name", "Unique listing identifier and its title"],
          ["host_id, host_name", "The host who owns the listing"],
          ["neighbourhood_group, neighbourhood", "Borough and neighbourhood (e.g. Manhattan / Midtown)"],
          ["latitude, longitude", "GPS coordinates of the listing"],
          ["room_type", "Entire home/apt, Private room, or Shared room"],
          ["price, minimum_nights", "Nightly price (USD) and minimum booking length"],
          ["number_of_reviews, reviews_per_month, last_review", "Review activity and recency"],
          ["calculated_host_listings_count, availability_365", "Host's total listings; days available per year"],
        ],
        [2400, 6600],
      ),
      p(""),
      p("This dataset was chosen because it is realistically messy (missing values, invalid prices, unreviewed listings) and rich enough to support genuine business questions about pricing, revenue, and demand across a city — exactly the kind of scenario a data engineer is asked to support.", { italics: true, size: 20, color: "555555" }),

      // ---- 2. System Architecture ----
      h1("2. System Architecture"),
      p("The pipeline follows the classic Extract → Clean/Validate → Transform → Load pattern introduced in the course, landing data first in a staging area before it is modelled into the analytical schema. Apache Airflow sits on top of every stage, scheduling and monitoring execution rather than participating in the data movement itself."),
      imgPara("architecture.png", 1507, 427),
      caption("Figure 1 — End-to-end architecture: source file → Python ETL stages → PostgreSQL (staging / core / marts) → analytical queries, all orchestrated by Airflow."),
      p("Three PostgreSQL schemas separate concerns cleanly:"),
      bullet("staging — an untouched copy of the raw CSV (raw_listings) plus a cleaned, validated version (cleaned_listings); this preserves an audit trail and lets any stage be re-run independently."),
      bullet("core — the normalized star schema (fact_listings plus four dimension tables) that analysts query."),
      bullet("marts — a pre-aggregated neighbourhood_summary table built for fast, repeated reporting."),

      // ---- 3. Database Schema ----
      h1("3. Database Schema (ER Diagram)"),
      p("The source CSV is a single flat file with repeating attributes (e.g. neighbourhood_group and host_name are repeated on every row that shares a borough or host). To remove this redundancy and reach a normalized design, the schema separates the data into a central fact table, fact_listings, and four supporting dimension tables — dim_host, dim_neighbourhood, dim_room_type, and dim_date — each holding one unique row per real-world entity and referenced from the fact table by a surrogate key."),
      imgPara("erd.png", 1019, 886, 480),
      caption("Figure 2 — Entity-relationship diagram of the core star schema. Every dimension connects directly to fact_listings (1-to-many), keeping analytical queries to simple fact-join-dimension patterns."),
      p("Design rationale:"),
      bullet("dim_host and dim_neighbourhood remove repeating groups from the raw file, satisfying 3NF while keeping join patterns simple (a defining property of dimensional modelling, as covered in Session 3)."),
      bullet("last_review_date_key is nullable because 10,052 listings in the raw data have never received a review — this is a legitimate business state, not missing data, and is preserved rather than defaulted."),
      bullet("Surrogate integer keys (SERIAL) are used for every dimension instead of natural keys, so dimension attributes (e.g. a host's display name) can change over time without breaking fact table references."),

      // ---- 4. ETL Workflow ----
      h1("4. ETL Workflow"),
      h2("4.1 Extract"),
      p("extract.py reads the source CSV with pandas and loads it, completely unmodified, into staging.raw_listings. Keeping an untouched copy of the source is what allows every later stage to be safely re-run without re-downloading or re-reading the original file."),
      h2("4.2 Clean & Validate"),
      p("clean.py reads staging.raw_listings and applies the following rules, each targeting a specific data-quality issue found during profiling of the raw file:"),
      simpleTable(
        ["Issue found", "Rows affected", "Rule applied"],
        [
          ["Missing listing name", "16", "Filled with 'Unnamed Listing'"],
          ["Missing host name", "21", "Filled with 'Unknown Host'"],
          ["Never reviewed (last_review / reviews_per_month null)", "10,052", "reviews_per_month set to 0; last_review kept NULL (valid state)"],
          ["Invalid price (price <= 0)", "11", "Row dropped"],
          ["Invalid minimum_nights (<= 0)", "0 (checked, none found)", "Row would be dropped"],
          ["GPS coordinates outside NYC bounding box", "0 (checked, none found)", "Row would be dropped"],
          ["Duplicate listing id", "0 (checked, none found)", "Duplicate rows would be dropped, keeping the first occurrence"],
        ],
        [3600, 1800, 3600],
      ),
      p(""),
      p("The result, staging.cleaned_listings, has 48,884 rows (11 fewer than the source, all removed for having a non-positive price)."),
      h2("4.3 Transform"),
      p("transform.py implements the four required transformations:"),
      bullet("Derived column — price_category buckets every listing into Budget (< $75), Mid-range ($75–199), or Luxury (≥ $200)."),
      bullet("Derived column — availability_status buckets availability_365 into Rarely / Occasionally / Highly Available, alongside a derived business metric, estimated_annual_revenue = price × (365 − availability_365)."),
      bullet("Date formatting — last_review is parsed into a proper DATE, expanded into a full dim_date row (year, month, month name, weekday, quarter), and used to compute days_since_last_review relative to a fixed reference date (2019-07-09, the day after the most recent review in the dataset)."),
      bullet("Aggregation — a neighbourhood-level summary (total listings, average price, average availability, total estimated revenue) is computed with a pandas group-by and written to marts.neighbourhood_summary."),
      h2("4.4 Load"),
      p("load.py loads the four dimension tables first (load_dimensions), then merges the transformed fact rows against each dimension's surrogate key and inserts them into core.fact_listings (load_fact). The aggregate table is loaded separately (load_marts). Splitting dimensions and fact into two functions mirrors real practice: the fact table's foreign keys cannot be populated until the dimension rows — and their surrogate keys — already exist."),

      // ---- 5. Airflow DAG ----
      h1("5. Airflow DAG Explanation"),
      p("The pipeline is orchestrated by the airbnb_dw_pipeline DAG (dags/airbnb_dw_pipeline_dag.py), scheduled to run daily. It has 8 tasks — more than the minimum of 5 — with explicit dependencies, including a fan-out after load_dimensions and a fan-in before the final task."),
      imgPara("dag.png", 1617, 145, 620),
      caption("Figure 3 — Airflow task graph for airbnb_dw_pipeline."),
      code(
        "create_schema >> extract_raw_data >> clean_and_validate >> transform_data\n" +
        "transform_data >> load_dimensions >> load_fact\n" +
        "load_dimensions >> load_marts\n" +
        "[load_fact, load_marts] >> run_analytical_queries"
      ),
      p("load_fact and load_marts both depend only on load_dimensions having completed, so Airflow can run them in parallel; run_analytical_queries then waits for both to finish before executing the analytical SQL. Each PythonOperator task calls a single, independently-testable function from the scripts/ package (the same functions used by run_pipeline.py for local testing), and default_args configure 2 retries with a 5-minute delay plus failure-email alerting, consistent with the orchestration benefits (automatic retries, alerting, dependency management) covered in Topic 3."),

      // ---- 6. Sample Query Results ----
      h1("6. Sample Query Results"),
      p("The following results were produced by running sql/02_analytical_queries.sql against the fully loaded database."),
      h2("6.1 Revenue by borough"),
      simpleTable(
        ["Borough", "Listings", "Avg. price ($)", "Total est. annual revenue ($)"],
        [
          ["Manhattan", "21,660", "196.88", "980,174,785"],
          ["Brooklyn", "20,095", "124.44", "632,908,801"],
          ["Queens", "5,666", "99.52", "119,324,032"],
          ["Bronx", "1,090", "87.58", "17,946,934"],
          ["Staten Island", "373", "114.81", "6,564,156"],
        ],
      ),
      p("Manhattan dominates both listing volume and average price, generating over 3.7× the estimated revenue of the next-highest borough, Brooklyn.", { size: 20 }),
      h2("6.2 Price and availability by room type"),
      simpleTable(
        ["Room type", "Listings", "Avg. price ($)", "Avg. availability (days/yr)", "Avg. reviews"],
        [
          ["Entire home/apt", "25,407", "211.81", "111.9", "22.8"],
          ["Private room", "22,319", "89.81", "111.2", "24.1"],
          ["Shared room", "1,158", "70.25", "161.9", "16.6"],
        ],
      ),
      p("Shared rooms are cheapest but sit available for far longer on average (161.9 vs ~111 days/year) — a signal of comparatively weak demand for that room type.", { size: 20 }),
      h2("6.3 Review recency by price category"),
      simpleTable(
        ["Price category", "Never reviewed", "Stale (>1yr)", "Reviewed within 1yr", "Total"],
        [
          ["Mid-range", "4,524", "5,312", "15,397", "25,233"],
          ["Budget", "2,660", "2,844", "8,362", "13,866"],
          ["Luxury", "2,867", "1,555", "5,363", "9,785"],
        ],
      ),
      p("Luxury listings have the highest never-reviewed rate proportionally, consistent with lower booking volume at higher price points.", { size: 20 }),
      p("Two further queries (top 10 neighbourhoods by price, top 10 hosts by estimated revenue) and the neighbourhood oversupply query are included in full in sql/02_analytical_queries.sql and were verified to run correctly against the loaded schema."),

      // ---- 7. Challenges & Lessons Learned ----
      h1("7. Challenges and Lessons Learned"),
      bullet("Distinguishing 'missing' from 'not applicable': 10,052 listings have no last_review because they have never been reviewed, not because data was lost. Treating this as a default value (e.g. 0 reviews_per_month is correct, but a fabricated last_review date would not be) required keeping the date column nullable throughout the star schema rather than coercing it."),
      bullet("Dimension/fact load ordering: because fact_listings' foreign keys reference surrogate keys generated when the dimension tables are loaded, load_dimensions must fully complete before load_fact begins — this is enforced explicitly in the DAG rather than left implicit, avoiding a subtle race condition that would otherwise only surface intermittently."),
      bullet("Choosing a stable reference date: computing 'days since last review' needed a fixed anchor date rather than 'today', since the pipeline must produce identical, reproducible output on every re-run regardless of when it happens to execute."),
      bullet("Balancing normalization with query simplicity: a fully normalized snowflake design (e.g. splitting neighbourhood_group into its own table referenced by dim_neighbourhood) would reduce redundancy further, but was intentionally avoided in favour of a star schema, so that every analytical query needs only a single fact-to-dimension join."),
      bullet("Validating cleaning rules against real profiling output, not assumptions: an early version of the cleaning script defaulted missing prices to the dataset average; profiling showed only 11 such rows, all with price = 0, so dropping them (as clearly invalid records) was more defensible than inventing a price."),
    ],
  }],
});

Packer.toBuffer(doc).then((buf) => {
  fs.writeFileSync("Technical_Report.docx", buf);
  console.log("Report written.");
});
