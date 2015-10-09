package io.druid.firehose.zeromq;

import java.nio.*;
import java.nio.charset.*;
import java.util.*;

import com.google.common.base.Charsets;
import com.metamx.common.parsers.ParseException;
import com.metamx.common.parsers.Parser;
import io.druid.data.input.ByteBufferInputRowParser;
import io.druid.data.input.InputRow;
import io.druid.data.input.impl.MapInputRowParser;
import io.druid.data.input.impl.ParseSpec;

public class RekeyStringInputRowParser implements ByteBufferInputRowParser
{
   private final ParseSpec parseSpec;
   private final MapInputRowParser mapParser;
   private final Parser<String, Object> parser;

   private Map<String, String> rekeyMap;

   private CharBuffer chars = null;

   public RekeyStringInputRowParser(ParseSpec parseSpec, Map<String, String> rekeyMap)
   {
      this.rekeyMap = rekeyMap;
      this.parseSpec = parseSpec;
      this.mapParser = new MapInputRowParser(parseSpec);
      this.parser = parseSpec.makeParser();
   }

   @Override
   public InputRow parse(ByteBuffer input)
   {
      return parseMap(buildStringKeyMap(input));
   }

   @Override
   public ParseSpec getParseSpec()
   {
      return parseSpec;
   }

   @Override
   public RekeyStringInputRowParser withParseSpec(ParseSpec parseSpec)
   {
      return new RekeyStringInputRowParser(parseSpec, new HashMap<String, String>());
   }

   private Map<String, Object> buildStringKeyMap(ByteBuffer input)
   {
      int payloadSize = input.remaining();

      if (chars == null || chars.remaining() < payloadSize) {
         chars = CharBuffer.allocate(payloadSize);
      }

      final CoderResult coderResult = Charsets.UTF_8.newDecoder()
         .onMalformedInput(CodingErrorAction.REPLACE)
         .onUnmappableCharacter(CodingErrorAction.REPLACE)
         .decode(input, chars, true);

      Map<String, Object> theMap;
      if (coderResult.isUnderflow()) {
         chars.flip();
         try {
            theMap = parseString(chars.toString());
         }
         finally {
            chars.clear();
         }
      } else {
         throw new ParseException("Failed with CoderResult[%s]", coderResult);
      }
      return theMap;
   }

   private Map<String, Object> parseString(String inputString)
   {
      Map<String, Object> result = parser.parse(inputString);
      Set<String> currentKeys = result.keySet();

      for (String key : currentKeys)
      {
         if (rekeyMap.containsKey(key))
         {
            Object temp = result.remove(key);
            result.put(rekeyMap.get(key), temp);
         }
      }

      return result;
   }

   public InputRow parse(String input)
   {
      return parseMap(parseString(input));
   }

   private InputRow parseMap(Map<String, Object> theMap)
   {
      return mapParser.parse(theMap);
   }
}
