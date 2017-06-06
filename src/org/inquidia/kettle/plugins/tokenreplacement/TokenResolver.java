/*! ******************************************************************************
 *
 * Inquidia Consulting
 *
 * Copyright (C) 2014-2017 by Inquidia Consulting : http://www.inquidia.com
 * *******************************************************************************
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.inquidia.kettle.plugins.tokenreplacement;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by openbi on 10/31/2014.
 */
public class TokenResolver {

  Map<String, String> tokenMap;

  public TokenResolver() {
    tokenMap = new HashMap<String,String>();
  }

  public void addToken( String tokenName, String tokenValue )
  {
    tokenMap.put( tokenName, tokenValue );
  }

  public String resolveToken( String tokenName )
  {
    return tokenMap.get( tokenName );
  }

}
