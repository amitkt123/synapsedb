package io.synapsedb.core.aggregation;

import io.synapsedb.core.document.Document;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Aggregation framework for analytical queries
 *
 * @author Amit Tiwari
 */
public class AggregationPipeline {

    private final List<AggregationStage> stages;

    public AggregationPipeline() {
        this.stages = new ArrayList<>();
    }

    public AggregationPipeline addStage(AggregationStage stage) {
        stages.add(stage);
        return this;
    }

    public AggregationResult execute(List<Document> documents) {
        List<Document> current = new ArrayList<>(documents);

        for (AggregationStage stage : stages) {
            current = stage.process(current);
        }

        return new AggregationResult(current);
    }

    // Stage builders
    public static MatchStage match(String field, Object value) {
        return new MatchStage(field, value);
    }

    public static GroupStage group(String groupByField) {
        return new GroupStage(groupByField);
    }

    public static SortStage sort(String field, boolean ascending) {
        return new SortStage(field, ascending);
    }

    public static LimitStage limit(int count) {
        return new LimitStage(count);
    }

    public static ProjectStage project(String... fields) {
        return new ProjectStage(Arrays.asList(fields));
    }

    // Aggregation stages
    public interface AggregationStage {
        List<Document> process(List<Document> documents);
    }

    public static class MatchStage implements AggregationStage {
        private final String field;
        private final Object value;

        public MatchStage(String field, Object value) {
            this.field = field;
            this.value = value;
        }

        @Override
        public List<Document> process(List<Document> documents) {
            return documents.stream()
                    .filter(doc -> {
                        Object fieldValue = doc.getField(field);
                        return fieldValue != null && fieldValue.equals(value);
                    })
                    .collect(Collectors.toList());
        }
    }

    public static class GroupStage implements AggregationStage {
        private final String groupByField;
        private final Map<String, Accumulator> accumulators;

        public GroupStage(String groupByField) {
            this.groupByField = groupByField;
            this.accumulators = new HashMap<>();
        }

        public GroupStage count(String outputField) {
            accumulators.put(outputField, new CountAccumulator());
            return this;
        }

        public GroupStage sum(String outputField, String inputField) {
            accumulators.put(outputField, new SumAccumulator(inputField));
            return this;
        }

        public GroupStage avg(String outputField, String inputField) {
            accumulators.put(outputField, new AvgAccumulator(inputField));
            return this;
        }

        public GroupStage min(String outputField, String inputField) {
            accumulators.put(outputField, new MinAccumulator(inputField));
            return this;
        }

        public GroupStage max(String outputField, String inputField) {
            accumulators.put(outputField, new MaxAccumulator(inputField));
            return this;
        }

        @Override
        public List<Document> process(List<Document> documents) {
            Map<Object, List<Document>> groups = documents.stream()
                    .collect(Collectors.groupingBy(doc ->
                            doc.getField(groupByField) != null ? doc.getField(groupByField) : "_null"));

            List<Document> results = new ArrayList<>();

            for (Map.Entry<Object, List<Document>> group : groups.entrySet()) {
                Document resultDoc = new Document();
                resultDoc.addField("_id", group.getKey());

                for (Map.Entry<String, Accumulator> acc : accumulators.entrySet()) {
                    Object accValue = acc.getValue().accumulate(group.getValue());
                    resultDoc.addField(acc.getKey(), accValue);
                }

                results.add(resultDoc);
            }

            return results;
        }
    }

    public static class SortStage implements AggregationStage {
        private final String field;
        private final boolean ascending;

        public SortStage(String field, boolean ascending) {
            this.field = field;
            this.ascending = ascending;
        }

        @Override
        public List<Document> process(List<Document> documents) {
            Comparator<Document> comparator = (d1, d2) -> {
                Object v1 = d1.getField(field);
                Object v2 = d2.getField(field);

                if (v1 == null && v2 == null) return 0;
                if (v1 == null) return ascending ? -1 : 1;
                if (v2 == null) return ascending ? 1 : -1;

                @SuppressWarnings("unchecked")
                int cmp = ((Comparable<Object>) v1).compareTo(v2);
                return ascending ? cmp : -cmp;
            };

            return documents.stream()
                    .sorted(comparator)
                    .collect(Collectors.toList());
        }
    }

    public static class LimitStage implements AggregationStage {
        private final int count;

        public LimitStage(int count) {
            this.count = count;
        }

        @Override
        public List<Document> process(List<Document> documents) {
            return documents.stream()
                    .limit(count)
                    .collect(Collectors.toList());
        }
    }

    public static class ProjectStage implements AggregationStage {
        private final List<String> fields;

        public ProjectStage(List<String> fields) {
            this.fields = fields;
        }

        @Override
        public List<Document> process(List<Document> documents) {
            return documents.stream()
                    .map(doc -> {
                        Document projected = new Document(doc.getId());
                        for (String field : fields) {
                            if (doc.hasField(field)) {
                                projected.addField(field, doc.getField(field));
                            }
                        }
                        return projected;
                    })
                    .collect(Collectors.toList());
        }
    }

    // Accumulators
    interface Accumulator {
        Object accumulate(List<Document> documents);
    }

    static class CountAccumulator implements Accumulator {
        @Override
        public Object accumulate(List<Document> documents) {
            return documents.size();
        }
    }

    static class SumAccumulator implements Accumulator {
        private final String field;

        SumAccumulator(String field) {
            this.field = field;
        }

        @Override
        public Object accumulate(List<Document> documents) {
            return documents.stream()
                    .map(doc -> doc.getField(field))
                    .filter(Objects::nonNull)
                    .filter(v -> v instanceof Number)
                    .mapToDouble(v -> ((Number) v).doubleValue())
                    .sum();
        }
    }

    static class AvgAccumulator implements Accumulator {
        private final String field;

        AvgAccumulator(String field) {
            this.field = field;
        }

        @Override
        public Object accumulate(List<Document> documents) {
            return documents.stream()
                    .map(doc -> doc.getField(field))
                    .filter(Objects::nonNull)
                    .filter(v -> v instanceof Number)
                    .mapToDouble(v -> ((Number) v).doubleValue())
                    .average()
                    .orElse(0.0);
        }
    }

    static class MinAccumulator implements Accumulator {
        private final String field;

        MinAccumulator(String field) {
            this.field = field;
        }

        @Override
        public Object accumulate(List<Document> documents) {
            return documents.stream()
                    .map(doc -> doc.getField(field))
                    .filter(Objects::nonNull)
                    .filter(v -> v instanceof Comparable)
                    .min((a, b) -> ((Comparable<Object>) a).compareTo(b))
                    .orElse(null);
        }
    }

    static class MaxAccumulator implements Accumulator {
        private final String field;

        MaxAccumulator(String field) {
            this.field = field;
        }

        @Override
        public Object accumulate(List<Document> documents) {
            return documents.stream()
                    .map(doc -> doc.getField(field))
                    .filter(Objects::nonNull)
                    .filter(v -> v instanceof Comparable)
                    .max((a, b) -> ((Comparable<Object>) a).compareTo(b))
                    .orElse(null);
        }
    }

    public static class AggregationResult {
        private final List<Document> documents;

        public AggregationResult(List<Document> documents) {
            this.documents = documents;
        }

        public List<Document> getDocuments() {
            return documents;
        }

        public int getCount() {
            return documents.size();
        }
    }
}

