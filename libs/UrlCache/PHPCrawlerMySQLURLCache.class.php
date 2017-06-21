<?php
/**
 * Class for caching/storing URLs/links in a MySQL-database-file.
 *
 * This class is supposed to work also on multiple sources, and we don't want to delete the url's, nor create one database for each source,
 * as in the case of SQLite.
 *
 * Crawler link cache setup could be done like this:
 * $crawler->setUrlCacheType(PHPCrawlerUrlCacheTypes::URLCACHE_MYSQL, array(
      'db' => 'crawler_test',
      'host' => 'mysql_host',
        'username' => 'mysql_username',
        'password' => 'mysql_password',
        'create_tables' => true
      ), array(
          'crawler_uniqid' => array(
          'type' => 'bigint',
          'value' => $crawlerId,
          'filter' => true
        ),
          'is_article' => array(
          'type' => 'integer',
          'value' => 0,
          'filter' => false
        )
      ));
 *
 * Then you may call go() or goMultiProcessed().
 *
 * @package phpcrawl
 * @internal
 */
class PHPCrawlerMySQLURLCache extends PHPCrawlerURLCacheBase
{
  /**
   * PDO-object for querying MySQL.
   *
   * @var PDO
   */
  protected $PDO;

  /**
   * Prepared statement for inserting URLS into the db-file as PDOStatement-object.
   *
   * @var PDOStatement
   */
  protected $PreparedInsertStatement;

  protected $mysql_db;
  protected $mysql_host;
  protected $mysql_username;
  protected $mysql_password;

  /**
   * For MySQL link cache type, will contain values for some default fields.
   * Usually will only contain the crawler_uniqid, used during Insert and also during search, to only lookup urls through those pertaining to the current website.
   *
   * @var array $db_filters
   */
  protected $db_filters = null;

  protected $db_analyzed = false;

  /**
   * Initiates an mysql-URL-cache.
   *
   * @param string $dbName          The MySQL database to use
   * @param bool   $create_tables   Defines whether all necessary tables should be created
   */
  public function __construct($config, $dbFilters = null)
  {
    $this->mysql_db = $config['db'];
    $this->mysql_host = $config['host'];
    $this->mysql_username = $config['username'];
    $this->mysql_password = $config['password'];
    if (!isset($config['create_tables']))
      $config['create_tables'] = false;

    $this->db_filters = $dbFilters;

    // Initialize the missing parameters from filter config
    foreach($this->db_filters as $fld => &$fldConfig) {
      if ( ! isset($fldConfig['type']) )
        throw new \Exception('Missing type for field "'.$fld.'" in url cache configuration');
      if ( ! isset($fldConfig['create']) )
        $fldConfig['create'] = true;
      if ( ! isset($fldConfig['filter']) )
        $fldConfig['filter'] = false;
    }

    $this->openConnection($config['create_tables']);
  }

  private function getDefaultFilters() {
    $filters = array();
    foreach($this->db_filters as $fld=>$config) {
      if ($config['filter'])
        $filters[] = "$fld = ".$config['value'];
    }
    return implode(' AND ', $filters);
  }

  public function getUrlCount()
  {
    $sql = "SELECT count(id) AS sum FROM urls WHERE processed = 0";
    $sql .= ' AND '.$this->getDefaultFilters();

    $Result = $this->PDO->query($sql);
    $row = $Result->fetch(PDO::FETCH_ASSOC);
    $Result->closeCursor();
    return $row["sum"];
  }

  /**
   * Returns the next URL from the cache that should be crawled.
   *
   * @return PhpCrawlerURLDescriptor An PhpCrawlerURLDescriptor or NULL if currently no
   *                                 URL to process.
   */
  public function getNextUrl()
  {
    PHPCrawlerBenchmark::start("fetching_next_url_from_MySQLcache");

    $ok = $this->PDO->exec("START TRANSACTION");

    // Get row with max priority-level
    $sql = "SELECT max(priority_level) AS max_priority_level FROM urls WHERE in_process = 0 AND processed = 0";
    $sql .= ' AND '.$this->getDefaultFilters();

    //echo $sql."\r\n";
    $Result = $this->PDO->query($sql);
    $row = $Result->fetch(PDO::FETCH_ASSOC);

    if ( is_null($row["max_priority_level"]) )
    {
      $Result->closeCursor();
      $this->PDO->exec("COMMIT;");
      return null;
    }

    $sql = "SELECT * FROM urls WHERE priority_level = ".$row["max_priority_level"]." and in_process = 0 AND processed = 0";
    $sql .= ' AND '.$this->getDefaultFilters();
    $sql .= ' LIMIT 1';

    $Result = $this->PDO->query($sql);
    $row = $Result->fetch(PDO::FETCH_ASSOC);
    $Result->closeCursor();

    // Update row (set in process-flag)
    $this->PDO->exec("UPDATE urls SET in_process = 1 WHERE id = ".$row["id"]);

    $this->PDO->exec("COMMIT");
    PHPCrawlerBenchmark::stop("fetching_next_url_from_mysqlcache");

    // Return URL
    return new PHPCrawlerURLDescriptor($row["url_rebuild"], $row["link_raw"], $row["linkcode"], $row["linktext"], $row["refering_url"], $row["url_link_depth"]);
  }

  /**
   * Has no function in this class
   */
  public function getAllURLs()
  {
  }

  /**
   * Removes all URLs and all priority-rules from the URL-cache.
   */
  public function clear()
  {
    throw new \Exception('The urls table is not meant to be emptied: it will work as a permanent repository of all urls ever scanned');

     try {
       $sql = "DELETE FROM urls";
       $sql .= ' WHERE '.$this->getDefaultFilters();
        $this->PDO->exec($sql);
        //$this->PDO->exec("VACUUM;");
     } catch (PDOException $e){
       
    } catch (Exception $e){
      
    }
  }

  /**
   * Adds an URL to the url-cache
   *
   * @param PHPCrawlerURLDescriptor $UrlDescriptor
   */
  public function addURL(PHPCrawlerURLDescriptor $UrlDescriptor)
  {
    if ($UrlDescriptor == null) return;

    // Hash of the URL
    $map_key = md5($UrlDescriptor->url_rebuild);

    // Get priority of URL
    $priority_level = $this->getUrlPriority($UrlDescriptor->url_rebuild);

    $this->createPreparedInsertStatement();

    // Insert URL via prepared statement
    try
    {

      $values = array(
          ":priority_level" => $priority_level,
          ":distinct_hash" => $map_key,
          ":link_raw" => $UrlDescriptor->link_raw,
          ":linkcode" => $UrlDescriptor->linkcode,
          ":linktext" => $UrlDescriptor->linktext,
          ":refering_url" => $UrlDescriptor->refering_url,
          ":url_rebuild" => $UrlDescriptor->url_rebuild,
          ":is_redirect_url" => $UrlDescriptor->is_redirect_url,
          ":url_link_depth" => $UrlDescriptor->url_link_depth
      );

      foreach($this->db_filters as $fieldName => $config) {
        $values[":$fieldName"] = $config['value'];
      }

      $res = $this->PreparedInsertStatement->execute($values);
      /*echo 'INSERTING url... in '.__FUNCTION__."\r\n";
      echo 'Result: ';var_dump($res);echo "\r\n";
      print_r($this->PDO->errorInfo());
      echo "\r\n"; */
    }
    catch (\Exception $e)
    {
      $this->createPreparedInsertStatement(true);
      $this->addURL($UrlDescriptor);
    }
  }

  /**
   * Adds an bunch of URLs to the url-cache
   *
   * @param array $urls  A numeric array containing the URLs as PHPCrawlerURLDescriptor-objects
   */
  public function addURLs($urls)
  {
    PHPCrawlerBenchmark::start("adding_urls_to_mysqlcache");
    try {
      $this->PDO->exec("START TRANSACTION");

      $cnt = count($urls);
      for ($x=0; $x<$cnt; $x++)
      {
        if ($urls[$x] != null)
        {
          $this->addURL($urls[$x]);
        }

        // Commit after 1000 URLs (reduces memory-usage)
        if ($x%1000 == 0 && $x > 0)
        {
          $this->PDO->exec("COMMIT");
          $this->PDO->exec("START TRANSACTION");
        }
      }

      $this->PDO->exec("COMMIT");
      $this->PreparedInsertStatement->closeCursor();

      if ($this->db_analyzed == false)
      {
        //$this->PDO->exec("ANALYZE");
        $this->db_analyzed = true;
      }
    } catch (PDOException $e){
       
    } catch (Exception $e){
      
    }

    PHPCrawlerBenchmark::stop("adding_urls_to_mysqlcache");
  }

  /**
   * Marks the given URL in the cache as "followed"
   *
   * @param PHPCrawlerURLDescriptor $UrlDescriptor
   */
  public function markUrlAsFollowed(PHPCrawlerURLDescriptor $UrlDescriptor, $http_code)
  {
    PHPCrawlerBenchmark::start("marking_url_as_followes");
    $hash = md5($UrlDescriptor->url_rebuild);
    try {
      $sql = "UPDATE urls SET processed = 1, in_process = 0, http_code = " . $http_code . " WHERE ".$this->getDefaultFilters()." AND distinct_hash = '".$hash."'";
      $this->PDO->exec($sql);
    } catch (Exception $e) {

    }
    PHPCrawlerBenchmark::stop("marking_url_as_followes");
  }

  /**
   * Checks whether there are URLs left in the cache that should be processed or not.
   *
   * @return bool
   */
  public function containsURLs()
  {
    PHPCrawlerBenchmark::start("checking_for_urls_in_cache");
    try {
      $sql = "SELECT id FROM urls WHERE ".$this->getDefaultFilters()." AND (processed = 0 OR in_process = 1) LIMIT 1";
      $Result = $this->PDO->query($sql);
      /*echo $sql."\r\n";
      var_dump($Result);echo "\r\n";
      print_r($this->PDO->errorInfo());
      echo "\r\n";*/

      $has_columns = $Result->fetchColumn();

      $Result->closeCursor();

      PHPCrawlerBenchmark::stop("checking_for_urls_in_cache");

      if ($has_columns != false) {
        return true;
      } else
        return false;

    } catch (\PDOException $e){

    } catch (Exception $e){

    }
  }

  /**
   * Cleans/purges the URL-cache from inconsistent entries.
   */
  public function purgeCache()
  {
    // Set "in_process" to 0 for all URLs
    try {
      $this->PDO->exec("UPDATE urls SET in_process = 0 WHERE ".$this->getDefaultFilters());
    } catch (PDOException $e){
       
    } catch (Exception $e){
      
    }
  }


  protected function getDefaultColumns() {
    return array(
        'id' => 'integer PRIMARY KEY AUTO_INCREMENT',
        'in_process' => 'bool DEFAULT 0',
        'processed' => 'bool DEFAULT 0',
        'priority_level' => 'integer',
        'distinct_hash' => 'char(32) NOT NULL UNIQUE',
        'link_raw' => 'TEXT',
        'linkcode' => 'TEXT',
        'linktext' => 'TEXT',
        'refering_url' => 'TEXT',
        'url_rebuild' => 'TEXT',
        'is_redirect_url' => 'bool',
        'http_code' => 'integer',
        'url_link_depth' => 'integer');
  }

  /**
   * Creates the mysql-db-file and opens connection to it.
   *
   * @param bool $create_tables Defines whether all necessary tables should be created
   */
  protected function openConnection($create_tables = false)
  {
    PHPCrawlerBenchmark::start("connecting_to_mysql_db");

    // create MySQL db if not exists
    try {
      $dbh = new PDO("mysql:host={$this->mysql_host}", $this->mysql_username, $this->mysql_password);
      $dbh->exec("CREATE DATABASE `{$this->mysql_db}`;");
      // or die(print_r($dbh->errorInfo(), true));
    } catch (PDOException $e) {
      // Probably db already exists
    }

    // Open mysql db
    try {
      $this->PDO = new PDO("mysql:dbname={$this->mysql_db};host={$this->mysql_host}", $this->mysql_username, $this->mysql_password);
    }
    catch (Exception $e) {
      $dbNotExisting = true;
      echo "MySQL connection error: ".$e->getMessage();
    }

    $this->PDO->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_SILENT); // ERRMODE_EXCEPTION
    $this->PDO->setAttribute(PDO::ATTR_TIMEOUT, 3000);

    if ($create_tables == true)
    {
      $columns = $this->getDefaultColumns();
      // Create url-table (if not exists)

      foreach($this->db_filters as $fieldName => $config) {
        $columns["$fieldName"] = $config['type'];
      }

      $sqlParts = array();
      foreach($columns as $cName => $cType) {
        $sqlParts[] = $cName.' '.$cType;
      }

      try {
        $this->PDO->exec("CREATE TABLE IF NOT EXISTS urls (".implode(', ', $sqlParts).");");

        // Create indexes (seems that indexes make the whole thingy slower)
        $this->PDO->exec("ALTER TABLE urls ADD INDEX index_crawler_uniqid (crawler_uniqid);");
        $this->PDO->exec("ALTER TABLE urls ADD INDEX index_priority_level (priority_level);");
        $this->PDO->exec("ALTER TABLE urls ADD INDEX index_distinct_hash (distinct_hash);");
        $this->PDO->exec("ALTER TABLE urls ADD INDEX index_in_process (in_process);");
        $this->PDO->exec("ALTER TABLE urls ADD INDEX index_processed (processed);");

        //$this->PDO->exec("ANALYZE;");
      } catch(\PDOException $e) {
        echo $e->getMessage();
      }

    }

    PHPCrawlerBenchmark::stop("connecting_to_mysql_db");
  }

  /**
   * Creates the prepared statement for insterting URLs into database (if not done yet)
   *
   * @param bool $recreate If TRUE, the prepared statement will get (re)created nevertheless
   */
  protected function createPreparedInsertStatement($recreate = false)
  {
    if ($this->PreparedInsertStatement == null || $recreate == true)
    {

      $columns = array(
          'priority_level',
          'distinct_hash',
          'link_raw',
          'linkcode',
          'linktext',
          'refering_url',
          'url_rebuild',
          'is_redirect_url',
          'url_link_depth');
      $values = array(
          ':priority_level',
          ':distinct_hash',
          ':link_raw',
          ':linkcode',
          ':linktext',
          ':refering_url',
          ':url_rebuild',
          ':is_redirect_url',
          ':url_link_depth');

      foreach($this->db_filters as $fieldName => $config) {
        $columns[] = "$fieldName";
        $values[] = ":$fieldName";
      }

      $sql = "INSERT INTO urls (".implode(', ', $columns).") VALUES (".implode(', ', $values).")";
      // Prepared statement for URL-inserts
      // Prepared statement for URL-inserts
      $this->PreparedInsertStatement = $this->PDO->prepare($sql);
    }
  }

  /**
   * Cleans up the cache after is it not needed anymore.
   */
  public function cleanup()
  {
    $this->PDO = null;
    $this->PreparedInsertStatement = null;
  }
}
?>
