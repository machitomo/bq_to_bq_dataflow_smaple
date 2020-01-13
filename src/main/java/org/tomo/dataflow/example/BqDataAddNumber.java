package org.tomo.dataflow.example;


import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;


/**
 * BQ(Original table) から BQ(Sub Table)へデータを加工して移し替え
 *
 * @author tomo
 *
 */
public class BqDataAddNumber {

  private static String SUB_TABLE = "";
  private static String ORIGINNAL_TABLE = "";

  /**
   * 実行オプション定義
   *
   * @author tomo
   *
   */
  public interface BqDataAddNumberOptions extends PipelineOptions {

    @Description("BigQuery table schema file")
    @Default.String("gs://<バケット名>/schema.json")
    String getSchemaFile();

    void setSchemaFile(String schemaFile);
  }

  /**
   * インプットデータの加工
   *
   * @author tomo
   *
   */
  public static class ParseFn extends DoFn<TableRow, TableRow> {

    @ProcessElement
    public void processElement(ProcessContext c) {
      // インプットデータの加工
      TableRow row = c.element();
      String id = row.get("id").toString();
      String value = row.get("value").toString();

      // 仮想テーブルの作成
      TableRow outputRow = new TableRow().set("id", id).set("value", value + " にゃーん " + id);

      // ProcessContextにBQのテーブル定義を書き込む
      c.output(outputRow);

    }
  }

  /**
   * パイプラインの作成と実行
   *
   * @param options
   * @throws IOException
   */
  static void runBqDataAddNumber(BqDataAddNumberOptions options) throws IOException {

    final Pipeline p = Pipeline.create(options);

    // アウトプットするBQのスキーマの作成
    InputStream is = ClassLoader.getSystemResourceAsStream("table_schema.json");
    ObjectMapper mapper = new ObjectMapper();
    List<Schema> schemaList = mapper.readValue(is, new TypeReference<List<Schema>>() {});

    List<TableFieldSchema> fields = new ArrayList<>();

    // スキーマ定義を設定する
    for (Schema schema : schemaList) {
      fields.add(new TableFieldSchema().setName(schema.getName()).setType(schema.getType())
          .setMode(schema.getMode()));
    }

    final TableSchema tableSchema = new TableSchema().setFields(fields);

    // BQからデータの読み込み
    // TODO sqlファイルから読み込ませるようにしたい。
    String query = "select id,value from [" + BqDataAddNumber.ORIGINNAL_TABLE + "]";
    PCollection<TableRow> inputRows =
        p.apply("ReadFromBQ", BigQueryIO.readTableRows().fromQuery(query));

    // 加工データの格納
    PCollection<TableRow> outputRows = inputRows.apply(ParDo.of(new ParseFn()));


    // 書き込み
    outputRows.apply("WriteToBQ",
        BigQueryIO.writeTableRows().to(BqDataAddNumber.SUB_TABLE).withSchema(tableSchema)
            .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
            .withWriteDisposition(WriteDisposition.WRITE_APPEND));

    // パイプラインの実行
    p.run().waitUntilFinish();
  }

  /**
   * メイン関数
   *
   * @param args
   * @throws IOException
   */
  public static void main(String[] args) throws IOException {
    // 実行オプションの作成
    BqDataAddNumberOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(BqDataAddNumberOptions.class);

    // TODO 環境変数から指定したけど、プロパティファイルとかの方がよかったかな？
    BqDataAddNumber.SUB_TABLE = System.getenv("SUB_TABLE");
    BqDataAddNumber.ORIGINNAL_TABLE = System.getenv("ORIGINNAL_TABLE");

    // パイプラインの作成と実行（あとは任せた！！！）
    runBqDataAddNumber(options);
  }
}

