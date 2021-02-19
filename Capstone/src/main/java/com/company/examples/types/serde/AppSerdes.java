package com.company.examples.types.serde;


import com.company.examples.types.product;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

import java.util.HashMap;
import java.util.Map;

public class AppSerdes extends Serdes {


    static final class productSerde extends WrapperSerde<product> {
        productSerde() {
            super(new JsonSerializer<>(), new JsonDeserializer<>());
        }
    }

    public static Serde<product> product() {
        productSerde serde = new productSerde();

        Map<String, Object> serdeConfigs = new HashMap<>();
        serdeConfigs.put(JsonDeserializer.VALUE_CLASS_NAME_CONFIG, product.class);
        serde.configure(serdeConfigs, false);

        return serde;
    }


}
