package io.synapsedb.core.durability;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.synapsedb.core.document.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;


/**
 * Serializes and deserializes Document objects to/from byte arrays for WAL storage.
 * Uses JSON format internally for flexibility and readability during recovery.
 *
 * Format: [id_length(4)][id(utf-8)][json_length(4)][json(utf-8)]
 *
 * @author Amit Tiwari
 */
public class DocumentSerialiser {
    private static final Logger logger = LoggerFactory.getLogger(DocumentSerialiser.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final org.slf4j.Logger log = LoggerFactory.getLogger(DocumentSerialiser.class);


    /**
     * Serialize a document to bytes
     */
    public static byte[] serialize(Document document) throws IOException, NullPointerException, IllegalArgumentException{
        Objects.requireNonNull(document, "Document cannot be null");
        if(!document.isValid()){
            logger.error("Document is not valid");
            throw new IllegalArgumentException("Document Id cannot be null or empty");
        }

        // Convert document to JSON and then to bytes
        return objectMapper.writeValueAsBytes(document);
    }

    public static Document deserialise(byte[] bytes) throws IOException{
        Objects.requireNonNull(bytes, "Bytes cannot be null");
        if (bytes.length == 0) {
            throw new IllegalArgumentException("Bytes array is empty");
        }

        // Deserialize from JSON bytes
        return objectMapper.readValue(bytes, Document.class);
    }
}
