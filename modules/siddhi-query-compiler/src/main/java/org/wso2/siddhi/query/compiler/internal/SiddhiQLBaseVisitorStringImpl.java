package org.wso2.siddhi.query.compiler.internal;

import org.antlr.v4.runtime.TokenStreamRewriter;
import org.antlr.v4.runtime.misc.NotNull;
import org.wso2.siddhi.query.compiler.SiddhiQLBaseVisitor;
import org.wso2.siddhi.query.compiler.SiddhiQLParser;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SiddhiQLBaseVisitorStringImpl extends SiddhiQLBaseVisitor {

    private Set<String> activeStreams = new HashSet<String>();

    private TokenStreamRewriter tokenStreamRewriter;

    public SiddhiQLBaseVisitorStringImpl(TokenStreamRewriter tokenStreamRewriter) {
        this.tokenStreamRewriter = tokenStreamRewriter;
    }

    @Override
    public Object visitParse(@NotNull SiddhiQLParser.ParseContext ctx) {
        return visit(ctx.execution_plan());
    }

    @Override
    public StringBuilder visitExecution_plan(@NotNull SiddhiQLParser.Execution_planContext ctx) {
        StringBuilder executionPlanText = new StringBuilder(" { \"ExecutionPlan\": [ ");

        for (SiddhiQLParser.Plan_annotationContext annotationContext : ctx.plan_annotation()) {
            executionPlanText.append((StringBuilder) visit(annotationContext)).append(", ");
        }
        for (SiddhiQLParser.Definition_streamContext streamContext : ctx.definition_stream()) {
            executionPlanText.append((StringBuilder) visit(streamContext)).append(", ");
        }
        for (SiddhiQLParser.Definition_tableContext tableContext : ctx.definition_table()) {
            executionPlanText.append((StringBuilder) visit(tableContext)).append(", ");
        }
        for (SiddhiQLParser.Definition_functionContext functionContext : ctx.definition_function()) {
            executionPlanText.append((StringBuilder) visit(functionContext)).append(", ");
        }
        for (SiddhiQLParser.Execution_elementContext executionElementContext : ctx.execution_element()) {
            executionPlanText.append((StringBuilder) visit(executionElementContext)).append(", ");
        }
        for (SiddhiQLParser.Definition_triggerContext triggerContext : ctx.definition_trigger()) {
            executionPlanText.append((StringBuilder) visit(triggerContext)).append(", ");
        }

        String exe_Text = tokenStreamRewriter.getTokenStream().getText(ctx.getStart(), ctx.getStop()).replaceAll("\\n", "\\\\n");
        executionPlanText.append(" { \"executionPlan_Text\":\"").append(exe_Text.replaceAll("\"", "\\\\\"")).append("\",");

        executionPlanText = new StringBuilder(executionPlanText.substring(0, executionPlanText.length() - 1));
        executionPlanText.append("}]}");

        return executionPlanText;
    }

    @Override
    public StringBuilder visitSource(@NotNull SiddhiQLParser.SourceContext ctx) {

        StringBuilder sourceText = new StringBuilder("{");
        List<String> streamId = new ArrayList<String>();
        List<String> inner = new ArrayList<String>();

        streamId.add("\"" + ctx.stream_id().getText() + "\"");
        if (ctx.inner != null) {
            inner.add("\"true\"");
        } else {
            inner.add("\"false\"");
        }
        sourceText.append("\"streamId\":").append(streamId).append(",");
        sourceText.append("\"innerStream\":").append(inner).append(",");

        sourceText = new StringBuilder(sourceText.substring(0, sourceText.length() - 1));
        sourceText.append("}");

        return sourceText;
    }

    @Override
    public Object visitDefinition_stream_final(@NotNull SiddhiQLParser.Definition_stream_finalContext ctx) {
        return visit(ctx.definition_stream());
    }

    @Override
    public StringBuilder visitDefinition_stream(@NotNull SiddhiQLParser.Definition_streamContext ctx) {

        StringBuilder streamText = new StringBuilder("  {\"Stream\":  ");

        streamText.append("  [{ \"streamId\":\"").append(ctx.source().stream_id().getText()).append("\" ,");

        String stream_Text = tokenStreamRewriter.getTokenStream().getText(ctx.getStart(), ctx.getStop()).replaceAll("\\n", "\\\\n");
        streamText.append(" \"stream_Text\":\"").append(stream_Text).append("\" ,");

        if (ctx.annotation().isEmpty()) {
            streamText.append(" \"annoName\": ").append("null").append(" ,");
            streamText.append(" \"annoElement\": ").append("null").append(" ,");
        } else {
            for (SiddhiQLParser.AnnotationContext annotationContext : ctx.annotation()) {
                streamText.append(" \"annoName\":\"").append(annotationContext.name().getText()).append("\" ,");
                if (annotationContext.annotation_element().isEmpty()) {
                    streamText.append(" \"annoElement\": ").append("null").append(" ,");
                } else {
                    for (SiddhiQLParser.Annotation_elementContext elementContext : annotationContext.annotation_element()) {
                        streamText.append(" \"annoElement\":\"").append(elementContext.property_value().getText()).append("\" ,");
                    }
                }
            }
        }

        streamText = new StringBuilder(streamText.substring(0, streamText.length() - 1));
        streamText.append(" }]}");

        return streamText;
    }

    @Override
    public Object visitDefinition_function_final(@NotNull SiddhiQLParser.Definition_function_finalContext ctx) {
        return visit(ctx.definition_function());
    }

    @Override
    public StringBuilder visitDefinition_function(@NotNull SiddhiQLParser.Definition_functionContext ctx) {
        StringBuilder functionText = new StringBuilder(" {  \"Function\":  ");

        functionText.append("  [{ \"functionName\":\"").append(ctx.function_name().getText()).append("\",");
        functionText.append("\"languageName\":\"").append(ctx.language_name().getText()).append("\",");

        functionText.append("\"attributeType\":\"").append(ctx.attribute_type().getText()).append("\",");
        String functionBody = tokenStreamRewriter.getTokenStream().getText(ctx.function_body().getStart(), ctx.function_body().getStop()).replaceAll("\\n", "\\\\n");
        functionText.append("\"functionBody\":\"").append(functionBody.replaceAll("\"", "\\\\\"")).append("\",");
        String function_Text = tokenStreamRewriter.getTokenStream().getText(ctx.getStart(), ctx.getStop()).replaceAll("\\n", "\\\\n");
        functionText.append("\"functionText\":\"").append(function_Text.replaceAll("\"", "\\\\\"")).append("\",");

        functionText = new StringBuilder(functionText.substring(0, functionText.length() - 1));
        functionText.append(" }]}");

        return functionText;
    }

    @Override
    public Object visitDefinition_trigger_final(@NotNull SiddhiQLParser.Definition_trigger_finalContext ctx) {
        return visitDefinition_trigger(ctx.definition_trigger());
    }

    @Override
    public StringBuilder visitDefinition_trigger(@NotNull SiddhiQLParser.Definition_triggerContext ctx) {
        StringBuilder triggerText = new StringBuilder(" {  \"Trigger\":  ");

        triggerText.append("  [{ \"triggerName\":\"").append(ctx.trigger_name().getText()).append("\",");
        if (ctx.time_value() != null) {
            triggerText.append(" \"triggerValue\":\"").append(ctx.time_value().getText()).append("\",");
        } else {
            triggerText.append(" \"triggerValue\":\"").append(ctx.string_value().getText()).append("\",");
        }

        triggerText.append(" \"triggerText\":\"").append(tokenStreamRewriter.getTokenStream().getText(ctx.getStart(), ctx.getStop()).replaceAll("\\n", "\\\\n")).append("\",");

        triggerText = new StringBuilder(triggerText.substring(0,triggerText.length() - 1));
        triggerText.append(" }]}");

        return triggerText;
    }

    @Override
    public Object visitDefinition_table_final(@NotNull SiddhiQLParser.Definition_table_finalContext ctx) {
        return visit(ctx.definition_table());
    }

    @Override
    public StringBuilder visitDefinition_table(@NotNull SiddhiQLParser.Definition_tableContext ctx) {
        StringBuilder tableText = new StringBuilder(" { \"Table\":  ");

        tableText.append("  [{ \"tableId\":\"").append(ctx.source().stream_id().getText()).append("\",");

        String table_Text = tokenStreamRewriter.getTokenStream().getText(ctx.getStart(), ctx.getStop()).replaceAll("\\n", "\\\\n");
        tableText.append(" \"table_Text\":\"").append(table_Text).append("\",");

        if (ctx.annotation().isEmpty()) {
            tableText.append(" \"annoName\": ").append("null").append(" ,");
            tableText.append(" \"annoElement\": ").append("null").append(" ,");
        } else {
            for (SiddhiQLParser.AnnotationContext annotationContext : ctx.annotation()) {
                tableText.append(" \"annoName\":\"").append(annotationContext.name().getText()).append("\" ,");
                tableText.append(" \"annoElement\":\"").append(annotationContext.annotation_element(2).property_value().getText()).append("\" ,");
            }
        }

        tableText = new StringBuilder(tableText.substring(0, tableText.length() - 1));
        tableText.append(" }]}");

        return tableText;
    }

    @Override
    public Object visitPartition_final(@NotNull SiddhiQLParser.Partition_finalContext ctx) {
        return visit(ctx.partition());
    }

    @Override
    public StringBuilder visitPartition(@NotNull SiddhiQLParser.PartitionContext ctx) {
        StringBuilder partitionText = new StringBuilder(" { \"Partition\":  ");

        for (SiddhiQLParser.Partition_with_streamContext with_streamContext : ctx.partition_with_stream()) {
            String pw_Text = tokenStreamRewriter.getTokenStream().getText(with_streamContext.getStart(), with_streamContext.getStop()).replaceAll("\\n", "\\\\n");
            partitionText.append("  [{  \"Partition_with_Text\":\"").append(pw_Text).append("\", ");
        }

        for (SiddhiQLParser.Partition_with_streamContext with_streamContext : ctx.partition_with_stream()) {
            partitionText.append(visitPartition_with_stream(with_streamContext));
        }

        partitionText.append(" \"Query_size\":").append(ctx.query().size()).append(",");

        for (int i = 0; i < ctx.query().size(); i++) {
            SiddhiQLParser.QueryContext queryContext = ctx.query().get(i);
            partitionText.append("\"Query_" + i + "\" : [").append(visitQuery(queryContext)).append("], ");
        }
        String p_Text = tokenStreamRewriter.getTokenStream().getText(ctx.getStart(), ctx.getStop()).replaceAll("\\n", "\\\\n");
        partitionText.append(" \"Partition_Text\":\"").append(p_Text).append("\",");

        partitionText = new StringBuilder(partitionText.substring(0, partitionText.length() - 1));
        partitionText.append(" }]}");

        return partitionText;
    }

    @Override
    public StringBuilder visitPartition_with_stream(@NotNull SiddhiQLParser.Partition_with_streamContext ctx) {
        StringBuilder partitionWith = new StringBuilder(" \"PartitionWith\": [{  ");

        partitionWith.append(" \"Partition_Stream\":\"").append(ctx.stream_id().getText()).append("\", ");

        List<String> condition = new ArrayList<String>();
        List<String> attribute = new ArrayList<String>();

        if (ctx.condition_ranges() != null) {
            for (SiddhiQLParser.Condition_rangeContext context : ctx.condition_ranges().condition_range()) {
                condition.add("\"" + tokenStreamRewriter.getTokenStream().getText(context.expression().getStart(),context.expression().getStop()) + "\"");
                attribute.add("\"" + context.string_value().getText() + "\"");
            }
            partitionWith.append(" \"condition\":").append(condition).append(", ");
            partitionWith.append(" \"attribute\":").append(attribute).append(" ");

        } else if (ctx.attribute() != null) {
            condition.add("null");
            attribute.add("\"" + ctx.attribute().getText() + "\"");
            partitionWith.append(" \"condition\":").append(condition).append(", ");
            partitionWith.append(" \"attribute\":").append(attribute).append("  ");
        }

        partitionWith = new StringBuilder(partitionWith.substring(0, partitionWith.length() - 1));
        partitionWith.append(" }],");

        return partitionWith;
    }

    @Override
    public StringBuilder visitAnonymous_stream(@NotNull SiddhiQLParser.Anonymous_streamContext ctx) {

        StringBuilder anonymousStream = new StringBuilder();

        if (ctx.anonymous_stream() != null) {
            anonymousStream.append(visit(ctx.anonymous_stream().query_input()));
        }

        return anonymousStream;
    }


    @Override
    public Object visitQuery_final(@NotNull SiddhiQLParser.Query_finalContext ctx) {
        return visit(ctx.query());
    }

    @Override
    public StringBuilder visitQuery(@NotNull SiddhiQLParser.QueryContext ctx) {
        StringBuilder queryText = new StringBuilder(" {  \"Query\": [{ ");

        if (ctx.annotation().isEmpty()) {
            queryText.append(" \"annotationElement\":\"").append("query").append("\",");
        } else {
            queryText.append("\"annotationElement\":\"").append(ctx.annotation().get(0).annotation_element().get(0).property_value().getText()).append("\",");
        }

        queryText.append("\"inputStream\":").append(visit(ctx.query_input())).append(",");

        if (ctx.query_section() != null) {
            queryText.append("\"selection\":\"").append(ctx.query_section().getText()).append("\",");
        }

        queryText.append("\"outputStream\":").append(visit(ctx.query_output())).append(",");

        String q_Text = tokenStreamRewriter.getTokenStream().getText(ctx.getStart(), ctx.getStop()).replaceAll("\\n", "\\\\n");
        queryText.append("\"query_Text\":\"").append(q_Text).append("\",");

        queryText = new StringBuilder(queryText.substring(0, queryText.length() - 1));
        queryText.append(" }]}");

        return queryText;
    }

    @Override
    public StringBuilder visitQuery_output(@NotNull SiddhiQLParser.Query_outputContext ctx) {

        StringBuilder query_Output = new StringBuilder();

        query_Output.append(visit(ctx.target().source())).append(",");

        if (ctx.INSERT() != null) {
            if (ctx.output_event_type() != null) {
                query_Output.append("\"Output_event_type\" : \"").append(ctx.output_event_type().getText()).append("\",");
            }
        } else if (ctx.DELETE() != null) {
            if (ctx.output_event_type() != null) {
                query_Output.append("\"Output_event_type\" : \"").append(ctx.output_event_type().getText()).append("\",");
                query_Output.append("\"expression\" : \"").append(ctx.expression().getText()).append("\",");

            } else {
                query_Output.append("\"expression\" : \"").append(ctx.expression().getText()).append("\",");
            }
        } else if (ctx.UPDATE() != null) {
            if (ctx.output_event_type() != null) {
                query_Output.append("\"Output_event_type\" : \"").append(ctx.output_event_type().getText()).append("\",");
                query_Output.append("\"expression\" : \"").append(ctx.expression().getText()).append("\",");

            } else {
                query_Output.append("\"expression\" : \"").append(ctx.expression().getText()).append("\",");
            }
        } else if (ctx.RETURN() != null) {
            if (ctx.output_event_type() != null) {
                query_Output.append("\"Output_event_type\" : \"").append(ctx.output_event_type().getText()).append("\",");
            }
        }
        query_Output = new StringBuilder(query_Output.substring(0, query_Output.length() - 1));

        return query_Output;
    }

    @Override
    public StringBuilder visitStandard_stream(@NotNull SiddhiQLParser.Standard_streamContext ctx) {

        StringBuilder standardStream_Text = new StringBuilder();

        standardStream_Text.append(visit(ctx.source())).append(",");

        if (ctx.pre_window_handlers != null) {
            standardStream_Text.append("\"pre_window_Handler\":\"").append(ctx.pre_window_handlers.getText()).append("\",");

        }
        if (ctx.window() != null && ctx.post_window_handlers != null) {
            standardStream_Text.append("\"post_window_Handler\":\"").append(ctx.post_window_handlers.getText()).append("\",");
        }

        standardStream_Text = new StringBuilder(standardStream_Text.substring(0, standardStream_Text.length() - 1));


        return standardStream_Text;
    }

    @Override
    public StringBuilder visitJoin_stream(@NotNull SiddhiQLParser.Join_streamContext ctx) {

        StringBuilder joinStreamText = new StringBuilder("  {");

        List<String> streamId = new ArrayList<String>();
        List<String> inner = new ArrayList<String>();

        streamId.add("\"" + ctx.left_source.source().stream_id().getText() + "\"");
        if (ctx.left_source.source().inner != null) {
            inner.add("\"true\"");
        } else {
            inner.add("\"false\"");
        }
        streamId.add("\"" + ctx.right_source.source().stream_id().getText() + "\"");
        if (ctx.right_source.source().inner != null) {
            inner.add("\"true\"");
        } else {
            inner.add("\"false\"");
        }


        joinStreamText.append("\"streamId\":").append(streamId).append(",");
        joinStreamText.append("\"innerStream\":").append(inner).append(",");
        joinStreamText.append("\"stream\":\"").append(ctx.getText()).append("\",");

        if (ctx.within_time() != null) {
            joinStreamText.append("\"timeConstant\":\"").append(ctx.within_time().getText()).append("\",");
        }

        if (ctx.expression() != null) {
            joinStreamText.append(" \"onCondition\":\"").append(ctx.expression().getText()).append("\",");
        }

        joinStreamText = new StringBuilder(joinStreamText.substring(0, joinStreamText.length() - 1));
        joinStreamText.append(" }");

        return joinStreamText;

    }

    @Override
    public StringBuilder visitJoin_source(@NotNull SiddhiQLParser.Join_sourceContext ctx) {

        StringBuilder joinStream_Text = new StringBuilder("[{ ");

        Source source = (Source) visit(ctx.source());

        String streamAlias = null;
        if (ctx.stream_alias() != null) {
            streamAlias = (String) visit(ctx.stream_alias());
            activeStreams.remove(ctx.source().getText());
            activeStreams.add(streamAlias);
        }
        joinStream_Text.append("\"joinSource\" : ").append(visit(ctx.source())).append(",");
        joinStream_Text.append("\"streamAlias\" : \"").append(streamAlias).append("\",");

        if (ctx.basic_source_stream_handlers() != null) {
            joinStream_Text.append("\"basic_source_stream_handlers: \'").append((ctx.basic_source_stream_handlers().getText())).append("\",");
        }

        if (ctx.window() != null) {
            joinStream_Text.append("\"window: \'").append((ctx.window().getText())).append("\",");
        }
        joinStream_Text = new StringBuilder(joinStream_Text.substring(0, joinStream_Text.length() - 1));
        joinStream_Text.append(" }]");

        return joinStream_Text;
    }

    @Override
    public StringBuilder visitPattern_stream(@NotNull SiddhiQLParser.Pattern_streamContext ctx) {

        StringBuilder patternStream_Text = new StringBuilder("[");
        patternStream_Text.append(visit(ctx.every_pattern_source_chain()));

        patternStream_Text = new StringBuilder(patternStream_Text.substring(0, patternStream_Text.length() - 1));
        patternStream_Text.append("]");
        return patternStream_Text;
    }

    @Override
    public StringBuilder visitEvery_pattern_source_chain(@NotNull SiddhiQLParser.Every_pattern_source_chainContext ctx) {

        StringBuilder every_pattern_sourceText = new StringBuilder();

        if (ctx.every_pattern_source_chain().size() == 1) { // '('every_pattern_source_chain')' within_time?
            every_pattern_sourceText.append(visit(ctx.every_pattern_source_chain(0)));
        } else if (ctx.every_pattern_source_chain().size() == 2) { // every_pattern_source_chain  '->' every_pattern_source_chain
            every_pattern_sourceText.append(visit(ctx.every_pattern_source_chain(0)));
            every_pattern_sourceText.append(visit(ctx.every_pattern_source_chain(1)));
        } else if (ctx.EVERY() != null) {
            if (ctx.pattern_source_chain() != null) { // EVERY '('pattern_source_chain ')' within_time?
                every_pattern_sourceText.append(visit(ctx.pattern_source_chain()));
            } else if (ctx.pattern_source() != null) { // EVERY pattern_source within_time?
                every_pattern_sourceText.append(visit(ctx.pattern_source()));
            }
        } else if (ctx.pattern_source_chain() != null) {  // pattern_source_chain
            every_pattern_sourceText.append(visit(ctx.pattern_source_chain()));
        }

        return every_pattern_sourceText;
    }

    @Override
    public StringBuilder visitPattern_source_chain(@NotNull SiddhiQLParser.Pattern_source_chainContext ctx) {
        StringBuilder pattern_sourceText = new StringBuilder();
        if (ctx.pattern_source_chain().size() == 1) {
            pattern_sourceText.append(visit(ctx.pattern_source_chain(0)));
        } else if (ctx.pattern_source_chain().size() == 2) {
            pattern_sourceText.append(visit(ctx.pattern_source_chain(0)));
            pattern_sourceText.append(visit(ctx.pattern_source_chain(1)));
        } else if (ctx.pattern_source() != null) {
            pattern_sourceText.append(visit(ctx.pattern_source()));
        }
        return pattern_sourceText;
    }

    @Override
    public StringBuilder visitStandard_stateful_source(@NotNull SiddhiQLParser.Standard_stateful_sourceContext ctx) {
        StringBuilder standard_source = new StringBuilder();
        standard_source.append(visit(ctx.basic_source()));
        return standard_source;
    }


    @Override
    public Object visitLogical_stateful_source(@NotNull SiddhiQLParser.Logical_stateful_sourceContext ctx) {

        StringBuilder logical_stateful_sourceText = new StringBuilder();

        if (ctx.NOT() != null) {
            if (ctx.AND() != null) {
                logical_stateful_sourceText.append(visit(ctx.standard_stateful_source(0)));
                logical_stateful_sourceText.append(visit(ctx.standard_stateful_source(1)));
            } else {
                logical_stateful_sourceText.append(visit(ctx.standard_stateful_source(0)));
            }
        } else if (ctx.AND() != null) {
            logical_stateful_sourceText.append(visit(ctx.standard_stateful_source(0)));
            logical_stateful_sourceText.append(visit(ctx.standard_stateful_source(1)));
        } else if (ctx.OR() != null) {
            logical_stateful_sourceText.append(visit(ctx.standard_stateful_source(0)));
            logical_stateful_sourceText.append(visit(ctx.standard_stateful_source(1)));
        }

        return logical_stateful_sourceText;
    }

    @Override
    public StringBuilder visitPattern_collection_stateful_source(@NotNull SiddhiQLParser.Pattern_collection_stateful_sourceContext ctx) {

        StringBuilder collection_stateful_sourceText = new StringBuilder();

        collection_stateful_sourceText.append(visit(ctx.standard_stateful_source()));

        return collection_stateful_sourceText;
    }

    @Override
    public StringBuilder visitSequence_stream(@NotNull SiddhiQLParser.Sequence_streamContext ctx) {
        StringBuilder sequence_stream = new StringBuilder("[");

        if (ctx.EVERY() != null) {
            sequence_stream.append(visit(ctx.sequence_source()));
        } else {
            sequence_stream.append(visit(ctx.sequence_source()));
        }

        sequence_stream.append(visit(ctx.sequence_source_chain()));

        sequence_stream = new StringBuilder(sequence_stream.substring(0, sequence_stream.length() - 1));
        sequence_stream.append("]");

        return sequence_stream;
    }

    @Override
    public StringBuilder visitSequence_source_chain(@NotNull SiddhiQLParser.Sequence_source_chainContext ctx) {
        StringBuilder sequence_source_chain = new StringBuilder();

        if (ctx.sequence_source_chain().size() == 1) {
            sequence_source_chain.append(visit(ctx.sequence_source_chain(0)));
        } else if (ctx.sequence_source_chain().size() == 2) {
            sequence_source_chain.append(visit(ctx.sequence_source_chain(0)));
            sequence_source_chain.append(visit(ctx.sequence_source_chain(1)));
        } else if (ctx.sequence_source() != null) {
            sequence_source_chain.append(visit(ctx.sequence_source()));
        }

        return sequence_source_chain;
    }

    @Override
    public StringBuilder visitSequence_collection_stateful_source(@NotNull SiddhiQLParser.Sequence_collection_stateful_sourceContext ctx) {

        StringBuilder sequence_collection = new StringBuilder();

        sequence_collection.append(visit(ctx.standard_stateful_source()));

        return sequence_collection;
    }

    @Override
    public StringBuilder visitBasic_source(@NotNull SiddhiQLParser.Basic_sourceContext ctx) {

        StringBuilder basicSource_Text = new StringBuilder();

        basicSource_Text.append(visit(ctx.source())).append(",");

        return basicSource_Text;
    }

    @Override
    public StringBuilder visitPlan_annotation(@NotNull SiddhiQLParser.Plan_annotationContext ctx) {
        StringBuilder annotation = new StringBuilder(" { ");

        for (SiddhiQLParser.Annotation_elementContext elementContext : ctx.annotation_element()) {
            annotation.append(" \"plan_annoElement\":\"").append(elementContext.property_value().string_value().getText()).append("\" ,");
        }

        annotation = new StringBuilder(annotation.substring(0, annotation.length() - 1));
        annotation.append("}");

        return annotation;
    }


    private class Source {

        private String streamId;
        private boolean isInnerStream;
    }


}
