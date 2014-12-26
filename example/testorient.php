<?php

$aDir = explode("/", __FILE__);
if (sizeof($aDir) > 8)
  $sInclude = implode("/", array_splice($aDir, 0, 8)) . DIRECTORY_SEPARATOR . "www" . DIRECTORY_SEPARATOR . "frameworks";
else
  $sInclude = implode("/", array_splice($aDir, 0, 4)) . DIRECTORY_SEPARATOR . "www" . DIRECTORY_SEPARATOR . "frameworks";

ini_set('include_path', ini_get("include_path") . ":" . $sInclude);
if (!defined("AK_ENV"))
  define("AK_ENV", "dev");
if (!defined("AK_DMZ"))
  define("AK_DMZ", "ankama");

ini_set("memory_limit", "4096M");
ini_set("mysqlnd.net_read_timeout", "31536000");

require_once("klassaka.php");
require_once("ankama.php");

if (!class_exists("Tanuki"))
  require_once("tanukin.php");

if (!class_exists("Modaka"))
  require_once("modaka.php");

require_once("thirdparty.php");

class Exchange_Types
{
  const DROP_ON_MONSTER = 1;
  const DROP_ON_PERCEPTOR = 2;
  const ACTION_ON_OBJECT = 3;
  const BUY_SHOP = 4;
  const GET_ON_PERCEPTOR = 5;
  const CRAFT = 6;
  const EXCHANGE = 7;
  const STORAGE = 8;
  const SHOP_ORGANISATION = 9;
  const INVENTORY_ADD_ITEM = 10;
  const BUY_HOUSE = 11;
  const BUY_IN_BID = 12;
  const SELL_IN_BID = 13;
  const SELL_TO_PNJ = 14;
  const EXCHANGE_KAMAS = 15;
  const TO_STORAGE = 16;
  const MULTICRAFT = 17;
  const MULTICRAFTPAY = 18;
  const DELETED = 19;
  const RIDE_ORGANISATION = 20;
  const TRADED_PNJ = 21;
  const DROP_ON_HARDCORE = 22;
  const DROP_ON_GROUND = 23;
  const GET_ON_GROUND = 24;
  const CRAFTED = 25;
  const LOST_ON_HARDCORE_OR_EPIC = 26;
  const OBJECT_USE = 27;
  const DESTROYED_BY_ADMIN = 28;
  const QUEST_REWARD = 29;
  const ARENA_DAILY_REWARD = 30;
  const RIDE_CERTIFICATE = 31;
  const RIDE_UNCERTIFICATE = 32;
  const PLAYER_GIFT_MAKE = 33;
  const PLAYER_GIFT_OPEN = 34;
  const MIMICRY_CONSUMMED = 35; //  "Ingrédients craft mimicry";
  const MIMICRY_CREATED = 36;	// 	"Résultat craft mimicry";
  const MIMICRY_REVERTED = 37; 	// 	"Revert mimicry";
  const TO_ALLIANCE_PRISM = 38;
  const FROM_ALLIANCE_PRISM = 39;
}

$oOrientDb = Klassaka_Nosql_OrientDB_OrientDB::Create("ODB_EXCHANGES");

//
$oOrientDb->Command("truncate class doc_dofus_exchange");
$oOrientDb->Command("truncate class DOFUS_ACCOUNT");
$oOrientDb->Command("truncate class DOFUS_TRASH");
$oOrientDb->Command("truncate class DOFUS_CHARACTER");
$oOrientDb->Command("truncate class INET_ADDRESS");
$oOrientDb->Command("truncate class DOFUS_HOUSE");
$oOrientDb->Command("truncate class CONNECTED_FROM");
$oOrientDb->Command("truncate class BELONGS_TO");
$oOrientDb->Command("truncate class DOFUS_MARKET");
$oOrientDb->Command("truncate class EXCHANGED_KAMAS_WITH");
$oOrientDb->Command("truncate class EXCHANGED_ITEM_WITH");
$oOrientDb->Command("truncate class PUT_IN_CHEST");
$oOrientDb->Command("truncate class TOOK_IN_CHEST");
$oOrientDb->Command("truncate class DROPPED");
$oOrientDb->Command("truncate class SELL_IN");
$oOrientDb->Command("truncate class BUY_IN");


$aMessage = json_decode('{"topic":"dofus2.exchanges","value":"2014-12-22T19:02:09+01:00 ankdo2ga20.ankama.internal 550500792 [GAME_ENGINE_THREAD] INFO  rsyslogObjectLogger  - 04002;32692836;1930070;190.137.187.198;32692836;1930070;190.137.187.198;6;2883603;;;;;14084;001400100800t0056001006008000500000009e000800000002b000600000001400100000000050038000000001037200000000d010600000000f0099000000080013300000000a03440000000080351000000007036300000000c033300000003o;1;0;","offset":3259886,"partition": 1,"key": -1}', true);
if ($aMessage["topic"] == "dofus2.exchanges")
{
  $sValue = $aMessage["value"];
  $sDate = substr($sValue, 0, 25);

  $sExchangeValue = explode("rsyslogObjectLogger  - ", $sValue)[1];
  $sExchangeValue = $sDate . ";" . $sExchangeValue;
  $aExchange = explode(";", $sExchangeValue);

  $iServerId = $aExchange[1] ? intval($aExchange[1]) : 0;
  $iAccountFromId = $aExchange[2] ? intval($aExchange[2]) : 0;
  $iAccountToId = $aExchange[5] ? intval($aExchange[5]) : 0;
  $iCharacterFromId = $aExchange[3] ? intval($aExchange[3]) : 0;
  $iCharacterToId = $aExchange[6] ? intval($aExchange[6]) : 0;
  $iMapId = $aExchange[9] ? intval($aExchange[9]) : 0;
  $iHouseId = $aExchange[11] ? intval($aExchange[11]) : 0;
  $iIpFrom = Klassaka_Net_Tools::IpToInt($aExchange[4]);
  $iIpTo = Klassaka_Net_Tools::IpToInt($aExchange[7]);
  $iItemId = $aExchange[14] ? intval($aExchange[14]) : 0;
  $iKamas = $aExchange[16] ? intval($aExchange[16]) : 0;

  $sType = $aExchange[8] ? intval($aExchange[8]) : 0;


  $sDocRecord = "#" . $oOrientDb->RecordCreate("doc_dofus_exchange", array(
      //exchange
      "exchange_date"                         => $aExchange[0],
      "exchange_server_id"                    => $iServerId,
      "exchange_account_id_from"              => $iAccountFromId,
      "exchange_character_id_from"            => $iCharacterFromId,
      "exchange_ip_from"                      => $iIpFrom,
      "exchange_account_id_to"                => $iAccountToId,
      "exchange_character_id_to"              => $iCharacterToId,
      "exchange_ip_to"                        => $iIpTo,
      "exchange_type"                         => $aExchange[8] ? intval($aExchange[8]) : 0,
      "exchange_map"                          => $iMapId,
      "exchange_item_id"                      => $iItemId,
      "exchange_item_quantity"                => $aExchange[16] ? intval($aExchange[16]) : 0,
      "exchange_kamas"                        => $iKamas,
      // dofus spé
      "dofus_exchange_chest_id"               => $aExchange[10] ? intval($aExchange[10]) : 0,
      "dofus_exchange_house_id"               => $iHouseId,
      "dofus_exchange_house_owner_account_id" => $aExchange[12] ? intval($aExchange[12]) : 0,
      "dofus_exchange_generic_pnj_id"         => $aExchange[13] ? intval($aExchange[13]) : 0,
      "dofus_exchange_item_fx"                => $aExchange[15],
    ));

  var_dump($sDocRecord, $sType);

  # account from
  $sAccountFromIdVertex = "";
  if ($iAccountFromId)
  {
    $aResult = $oOrientDb->Command("select * from DOFUS_ACCOUNT where account_id = $iAccountFromId");
    if (!sizeof($aResult))
    {
      var_dump('create account from');
      $aResult = $oOrientDb->Command("create vertex DOFUS_ACCOUNT set account_id=$iAccountFromId");
    }
    $sAccountFromIdVertex = $aResult[0]["@rid"];
    var_dump($sAccountFromIdVertex);
  }
  # account to
  $sAccountToIdVertex = "";
  if ($iAccountToId)
  {
    $aResult = $oOrientDb->Command("select * from DOFUS_ACCOUNT where account_id = $iAccountToId");
    if (!sizeof($aResult))
    {
      var_dump('create account to');
      $aResult = $oOrientDb->Command("create vertex DOFUS_ACCOUNT set account_id=$iAccountToId");
    }
    $sAccountToIdVertex = $aResult[0]["@rid"];
    var_dump($sAccountToIdVertex);
  }

  # character from
  $sCharacterFromIdVertex = "";
  if ($iCharacterFromId && $iServerId)
  {
    $aResult = $oOrientDb->Command("select * from DOFUS_CHARACTER where character_id = $iCharacterFromId and server_id = $iServerId");
    if (!sizeof($aResult))
    {
      var_dump('create character from');
      $aResult = $oOrientDb->Command("create vertex DOFUS_CHARACTER set character_id=$iCharacterFromId, server_id=$iServerId");
    }
    $sCharacterFromIdVertex = $aResult[0]["@rid"];
    var_dump($sCharacterFromIdVertex);
    // add history dofus character
    $oOrientDb->Command("update DOFUS_CHARACTER add history = $sDocRecord where @rid = $sCharacterFromIdVertex");
  }

  # character to
  $sCharacterToIdVertex = "";
  if ($iCharacterToId && $iServerId)
  {
    $aResult = $oOrientDb->Command("select * from DOFUS_CHARACTER where character_id = $iCharacterToId and server_id = $iServerId");
    if (!sizeof($aResult))
    {
      var_dump('create character to');
      $aResult = $oOrientDb->Command("create vertex DOFUS_CHARACTER set character_id=$iCharacterToId, server_id=$iServerId");
    }
    $sCharacterToIdVertex = $aResult[0]["@rid"];
    var_dump($sCharacterToIdVertex);
    // add history dofus character
    $oOrientDb->Command("update DOFUS_CHARACTER add history = $sDocRecord where @rid = $sCharacterToIdVertex");
  }

  #house
  $sHouseIdVertex = "";
  if ($iHouseId && $iMapId)
  {
    $aResult = $oOrientDb->Command("select * from DOFUS_HOUSE where house_id = $iHouseId and map_id = $iMapId");
    if (!sizeof($aResult))
    {
      var_dump('create house');
      $aResult = $oOrientDb->Command("create vertex DOFUS_HOUSE set house_id=$iHouseId, map_id=$iMapId");
    }
    $sHouseIdVertex = $aResult[0]["@rid"];
    var_dump($sHouseIdVertex);
    // add history dofus character
    $oOrientDb->Command("update DOFUS_HOUSE add history = $sDocRecord where @rid = $sHouseIdVertex");
  }

  #trash
  $sTrashIdVertex = "";
  if ($iMapId)
  {
    $aResult = $oOrientDb->Command("select * from DOFUS_TRASH where map_id = $iMapId");
    if (!sizeof($aResult))
    {
      var_dump('create trash');
      $aResult = $oOrientDb->Command("create vertex DOFUS_TRASH set map_id=$iMapId");
    }
    $sTrashIdVertex = $aResult[0]["@rid"];
    var_dump($sTrashIdVertex);
    // add history dofus character
    $oOrientDb->Command("update DOFUS_TRASH add history = $sDocRecord where @rid = $sTrashIdVertex");
  }

  #market ?
  $sMarketVertex = "";
  if (in_array($sType, array(Exchange_Types::SELL_IN_BID, Exchange_Types::BUY_IN_BID)))
  {
    $sMarketId = ""; // un champ prévu pour autre chose normalement
    if ($sMarketId)
    {
      $aResult = $oOrientDb->Command("select * from DOFUS_MARKET where id=$sMarketId");
      if (!sizeof($aResult))
      {
        var_dump('create market');
        $aResult = $oOrientDb->Command("create vertex DOFUS_MARKET set id=$sMarketId");
      }
      $sMarketVertex = $aResult[0]["@rid"];
      var_dump($sMarketVertex);
    }
  }

  #inet address from
  $sInetFromVertex = "";
  if ($iIpFrom)
  {
    $aResult = $oOrientDb->Command("select * from INET_ADDRESS where ip=$iIpFrom");
    if (!sizeof($aResult))
    {
      var_dump('create inet from');
      $aResult = $oOrientDb->Command("create vertex INET_ADDRESS set ip=$iIpFrom");
    }
    $sInetFromVertex = $aResult[0]["@rid"];
  }
  #inet address to
  $sInetToVertex = "";
  if ($iIpTo)
  {
    $aResult = $oOrientDb->Command("select * from INET_ADDRESS where ip=$iIpTo");
    if (!sizeof($aResult))
    {
      var_dump('create inet to');
      $aResult = $oOrientDb->Command("create vertex INET_ADDRESS set ip=$iIpTo");
    }
    $sInetToVertex = $aResult[0]["@rid"];
  }


  #region Edges

  // connected from
  $sConnectedFromEdge = "";
  if ($sAccountFromIdVertex && $sInetFromVertex)
  {
    $aResult = $oOrientDb->Command("select * from CONNECTED_FROM where out = $sAccountFromIdVertex and in = $sInetFromVertex");
    if (!sizeof($aResult))
    {
      var_dump('create CONNECTED_FROM from');
      $aResult = $oOrientDb->Command("create edge CONNECTED_FROM from $sAccountFromIdVertex to $sInetFromVertex");
    }
    $sConnectedFromEdge = $aResult[0]["@rid"];
  }

  // connected to
  $sConnectedToEdge = "";
  if ($sAccountToIdVertex && $sInetToVertex)
  {
    $aResult = $oOrientDb->Command("select * from CONNECTED_FROM where out = $sAccountToIdVertex and in = $sInetToVertex");
    if (!sizeof($aResult))
    {
      var_dump('create CONNECTED_FROM to');
      $aResult = $oOrientDb->Command("create edge CONNECTED_FROM from $sAccountToIdVertex to $sInetToVertex");
    }
    $sConnectedToEdge = $aResult[0]["@rid"];
  }

  // belong from
  $sBelongFromEdge = "";
  if ($sCharacterFromIdVertex && $sAccountFromIdVertex)
  {
    $aResult = $oOrientDb->Command("select * from BELONGS_TO where out = $sCharacterFromIdVertex and in = $sAccountFromIdVertex");
    if (!sizeof($aResult))
    {
      var_dump('create BELONGS_TO from');
      $aResult = $oOrientDb->Command("create edge BELONGS_TO from $sCharacterFromIdVertex to $sAccountFromIdVertex");
    }
    $sBelongFromEdge = $aResult[0]["@rid"];
  }

  // belong to
  $sBelongFromEdge = "";
  if ($sCharacterToIdVertex && $sAccountToIdVertex)
  {
    $aResult = $oOrientDb->Command("select * from BELONGS_TO where out = $sCharacterToIdVertex and in = $sAccountToIdVertex");
    if (!sizeof($aResult))
    {
      var_dump('create BELONGS_TO to');
      $aResult = $oOrientDb->Command("create edge BELONGS_TO from $sCharacterToIdVertex to $sAccountToIdVertex");
    }
    $sBelongFromEdge = $aResult[0]["@rid"];
  }

  // exchange kamas with
  $sEchangeKamasWithRID = "";
  if (in_array($sType, array(Exchange_Types::EXCHANGE_KAMAS)))
  {
    if ($iKamas && $sCharacterFromIdVertex && $sCharacterToIdVertex)
    {
      $aResult = $oOrientDb->Command("select * from EXCHANGED_KAMAS_WITH where out = $sCharacterFromIdVertex and in = $sCharacterToIdVertex");
      if (!sizeof($aResult))
      {
        var_dump('create EXCHANGED_KAMAS_WITH');
        $aResult = $oOrientDb->Command("create edge EXCHANGED_KAMAS_WITH from $sCharacterFromIdVertex to $sCharacterToIdVertex");
      }
      $sEchangeKamasWithRID = $aResult[0]["@rid"];
      $oOrientDb->Command("update EXCHANGED_KAMAS_WITH add history = $sDocRecord where @rid = $sEchangeKamasWithRID");
    }
  }

  // exchange item with
  $sEchangeItemWithRID = "";
  if (in_array($sType, array(Exchange_Types::EXCHANGE)))
  {
    if ($iItemId && $sCharacterFromIdVertex && $sCharacterToIdVertex)
    {
      $aResult = $oOrientDb->Command("select * from EXCHANGED_ITEM_WITH where out = $sCharacterFromIdVertex and in = $sCharacterToIdVertex");
      if (!sizeof($aResult))
      {
        var_dump('create EXCHANGED_ITEM_WITH');
        $aResult = $oOrientDb->Command("create edge EXCHANGED_ITEM_WITH from $sCharacterFromIdVertex to $sCharacterToIdVertex");
      }
      $sEchangeItemWithRID = $aResult[0]["@rid"];
      $oOrientDb->Command("update EXCHANGED_ITEM_WITH add history = $sDocRecord where @rid = $sEchangeItemWithRID");
    }
  }

  // put in chest selon exchange type ?
  $sPutInChestRID = "";
  if (in_array($sType, array(Exchange_Types::TO_STORAGE)))
  {
    if ($sCharacterFromIdVertex && $sHouseIdVertex)
    {
      $aResult = $oOrientDb->Command("select * from PUT_IN_CHEST where out = $sCharacterFromIdVertex and in = $sHouseIdVertex");
      if (!sizeof($aResult))
      {
        var_dump('create EXCHANGED_ITEM_WITH');
        $aResult = $oOrientDb->Command("create edge PUT_IN_CHEST from $sCharacterFromIdVertex to $sHouseIdVertex");
      }
      $sPutInChestRID = $aResult[0]["@rid"];
      $oOrientDb->Command("update PUT_IN_CHEST add history = $sDocRecord where @rid = $sPutInChestRID");
    }
  }

  // took in chest => selon exchange type ?
  $sTookInChestRID = "";
  if (in_array($sType, array(Exchange_Types::STORAGE)))
  {
    if ($sCharacterFromIdVertex && $sHouseIdVertex)
    {
      $aResult = $oOrientDb->Command("select * from TOOK_IN_CHEST where out = $sHouseIdVertex and in = $sCharacterFromIdVertex");
      if (!sizeof($aResult))
      {
        var_dump('create TOOK_IN_CHEST');
        $aResult = $oOrientDb->Command("create edge TOOK_IN_CHEST from $sHouseIdVertex to $sCharacterFromIdVertex");
      }
      $sTookInChestRID = $aResult[0]["@rid"];
      $oOrientDb->Command("update TOOK_IN_CHEST add history = $sDocRecord where @rid = $sTookInChestRID");
    }
  }

  // dropped
  $sDroppedRID = "";
  if (in_array($sType, array(Exchange_Types::DROP_ON_GROUND)))
  {
    if ($sCharacterFromIdVertex && $sTrashIdVertex)
    {
      $aResult = $oOrientDb->Command("select * from DROPPED where out = $sCharacterFromIdVertex and in = $sTrashIdVertex");
      if (!sizeof($aResult))
      {
        var_dump('create DROPPED');
        $aResult = $oOrientDb->Command("create edge DROPPED from $sCharacterFromIdVertex to $sTrashIdVertex");
      }
      $sDroppedRID = $aResult[0]["@rid"];
      $oOrientDb->Command("update DROPPED add history = $sDocRecord where @rid = $sDroppedRID");
    }
  }

  // SELL IN
  $sSellInRID = "";
  if ($sMarketVertex && in_array($sType, array(Exchange_Types::SELL_IN_BID)))
  {
    if ($sCharacterFromIdVertex && $sMarketVertex)
    {
      $aResult = $oOrientDb->Command("select * from SELL_IN where out = $sCharacterFromIdVertex and in = $sMarketVertex");
      if (!sizeof($aResult))
      {
        var_dump('create SELL_IN');
        $aResult = $oOrientDb->Command("create edge SELL_IN from $sCharacterFromIdVertex to $sMarketVertex");
      }
      $sSellInRID = $aResult[0]["@rid"];
      $oOrientDb->Command("update SELL_IN add history = $sDocRecord where @rid = $sSellInRID");
    }
  }
  // BUY IN
  $sBuyInRID = "";
  if ($sMarketVertex && in_array($sType, array(Exchange_Types::BUY_IN_BID)))
  {
    if ($sCharacterFromIdVertex && $sMarketVertex)
    {
      $aResult = $oOrientDb->Command("select * from BUY_IN where out = $sMarketVertex and in = $sCharacterFromIdVertex");
      if (!sizeof($aResult))
      {
        var_dump('create BUY_IN');
        $aResult = $oOrientDb->Command("create edge BUY_IN from $sMarketVertex to $sCharacterFromIdVertex");
      }
      $sBuyInRID = $aResult[0]["@rid"];
      $oOrientDb->Command("update BUY_IN add history = $sDocRecord where @rid = $sBuyInRID");
    }
  }


  #endregion


}

exit;

