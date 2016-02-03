package org.wso2.siddhi.query.compiler;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.TokenStreamRewriter;
import org.antlr.v4.runtime.tree.ParseTree;
import org.wso2.siddhi.query.compiler.internal.SiddhiErrorListener;
import org.wso2.siddhi.query.compiler.internal.SiddhiQLBaseVisitorStringImpl;


/**
 * Created by janaki on 1/5/16.
 */
public class SiddhiStringCompiler {
    public static StringBuilder parseString(String source) {

        ANTLRInputStream input = new ANTLRInputStream(source);
        SiddhiQLLexer lexer = new SiddhiQLLexer(input);
        lexer.removeErrorListeners();
        lexer.addErrorListener(SiddhiErrorListener.INSTANCE);

        CommonTokenStream tokens = new CommonTokenStream(lexer);
        TokenStreamRewriter tokenStreamRewriter = new TokenStreamRewriter(tokens);
        SiddhiQLParser parser = new SiddhiQLParser(tokens);
        //            parser.setErrorHandler(new BailErrorStrategy());
        parser.removeErrorListeners();
        parser.addErrorListener(SiddhiErrorListener.INSTANCE);
        ParseTree tree = parser.parse();

        SiddhiQLVisitor eval = new SiddhiQLBaseVisitorStringImpl(tokenStreamRewriter);

        return (StringBuilder) eval.visit(tree);

    }
}
