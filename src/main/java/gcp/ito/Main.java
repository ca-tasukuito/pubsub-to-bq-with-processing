package gcp.ito;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.*;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;

import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Blob;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.google.gson.*;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.TableFieldSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {
    private static String PUBSUB_TOPIC = "gcs-to-bq-json";
    private static String DATASET_NAME = "dataset_test";

    public interface MyPipelineOptions extends GcpOptions {

        @Description("ここに出力先のBigQueryテーブル名を指定")
        @Default.String("test")
        String getTableName();

        void setTableName(String value);
    }

    public static void main(String[] args) {
        MyPipelineOptions opt = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyPipelineOptions.class);
        Pipeline pipeline = Pipeline.create(opt);
        final String SRC_PUBSUB_TOPIC = String.format("projects/%s/topics/%s", opt.getProject(), PUBSUB_TOPIC);
        TableSchema tableschema = MakeSchema.create();
        //
        pipeline.apply(PubsubIO.readStrings().fromTopic(SRC_PUBSUB_TOPIC))
                .apply(ParDo.of(new ParsePubsubMessage()))
                .apply("WriteBigQuery",
                        BigQueryIO.writeTableRows().to(DATASET_NAME + "." + opt.getTableName()).withSchema(tableschema)
                                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));
        pipeline.run();
    }
}

class ParsePubsubMessage extends DoFn<String, TableRow> {
    @ProcessElement
    public void method(@Element String message, OutputReceiver<TableRow> out) throws Exception {
        JsonNode root = null;
        InputStream in = new ByteArrayInputStream(message.getBytes());
        try {
            ObjectMapper mapper = new ObjectMapper();
            root = mapper.readTree(in);
        } catch (IOException e) {

        }

        List<TableRow> rows = new ArrayList<>();
        // 頑張ってメッセージをパースする


        rows.forEach(out::output);
    }
}


// class MapToTableRow extends DoFn<String, TableRow> {

//     @ProcessElement
//     public void method(ProcessContext ctx) {
//         try {
//             Gson gson = new Gson();
//             String json = ctx.element();
//             Receipt receipt;
//             receipt = gson.fromJson(json, Receipt.class);
//             List<TableRow> rows = new ArrayList<>();

//             List<DigitalReceipt> digital_receipt = receipt.digitalReceipt;
//             String product, receipt_number, date, shop;
//             Integer product_value, amount;
//             for (DigitalReceipt re : digital_receipt) {
//                 shop = re.transaction.businessUnit.unitID.name;
//                 date = re.transaction.receiptDateTime;
//                 receipt_number = re.transaction.receiptNumber;
//                 List<LineItem> items = re.transaction.retailTransaction.lineItem;
//                 for (LineItem item : items) {
//                     product = item.sale.itemID.name;
//                     product_value = Integer.valueOf(item.sale.extendedAmount);
//                     amount = Integer.valueOf(item.sale.quantity);
//                     TableRow products = new TableRow();
//                     products.set("item", product);
//                     products.set("value", product_value);
//                     products.set("amount", amount);
//                     rows.add(new TableRow().set("date", date).set("shop", shop).set("receipt_number", receipt_number)
//                             .set("products", products));
//                 }
//             }
//             rows.forEach(ctx::output);
//         } catch (Exception e) {
//             // TODO: handle exception
//         }
//     }
// }

class MakeSchema {
    public static TableSchema create() {
        List<TableFieldSchema> fields;
        fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("date").setType("TIMESTAMP"));
        fields.add(new TableFieldSchema().setName("shop").setType("STRING"));
        fields.add(new TableFieldSchema().setName("receipt_number").setType("STRING"));
        fields.add(new TableFieldSchema().setName("products").setType("RECORD")
                .setFields(new ArrayList<TableFieldSchema>() {
                    {
                        add(new TableFieldSchema().setName("item").setType("STRING"));
                        add(new TableFieldSchema().setName("value").setType("INTEGER"));
                        add(new TableFieldSchema().setName("amount").setType("INTEGER"));
                    }
                }));
        TableSchema schema = new TableSchema().setFields(fields);
        return schema;
    }
}