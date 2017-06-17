<?php
/**
 * Class for caching/storing URLs/links in a MySQL-database-file.
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

  protected $db_analyzed = false;

  /**
   * Initiates an mysql-URL-cache.
   *
   * @param string $dbName          The MySQL database to use
   * @param bool   $create_tables   Defines whether all necessary tables should be created
   */
  public function __construct($config, $create_tables = false)
  {
    $this->mysql_db = $config['db'];
    $this->mysql_host = $config['host'];
    $this->mysql_username = $config['username'];
    $this->mysql_password = $config['password'];
    if (!isset($config['create_tables'])) $config['create_tables'] = false;
    $this->openConnection($config['create_tables']);
  }

  public function getUrlCount()
  {
    $Result = $this->PDO->query("SELECT count(id) AS sum FROM urls WHERE processed = 0;");
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
    $Result = $this->PDO->query("SELECT max(priority_level) AS max_priority_level FROM urls WHERE in_process = 0 AND processed = 0;");
    $row = $Result->fetch(PDO::FETCH_ASSOC);

    if ($row["max_priority_level"] == null)
    {
      $Result->closeCursor();
      $this->PDO->exec("COMMIT;");
      return null;
    }

    $Result = $this->PDO->query("SELECT * FROM urls WHERE priority_level = ".$row["max_priority_level"]." and in_process = 0 AND processed = 0;");
    $row = $Result->fetch(PDO::FETCH_ASSOC);
    $Result->closeCursor();

    // Update row (set in process-flag)
    $this->PDO->exec("UPDATE urls SET in_process = 1 WHERE id = ".$row["id"].";");

    $this->PDO->exec("COMMIT;");
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
     try {
        $this->PDO->exec("DELETE FROM urls;");
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
      $this->PreparedInsertStatement->execute(array(":priority_level" => $priority_level,
          ":distinct_hash" => $map_key,
          ":link_raw" => $UrlDescriptor->link_raw,
          ":linkcode" => $UrlDescriptor->linkcode,
          ":linktext" => $UrlDescriptor->linktext,
          ":refering_url" => $UrlDescriptor->refering_url,
          ":url_rebuild" => $UrlDescriptor->url_rebuild,
          ":is_redirect_url" => $UrlDescriptor->is_redirect_url,
          ":url_link_depth" => $UrlDescriptor->url_link_depth));
    }
    catch (Exception $e)
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
      $this->PDO->exec("BEGIN EXCLUSIVE TRANSACTION;");

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
          $this->PDO->exec("COMMIT;");
          $this->PDO->exec("BEGIN EXCLUSIVE TRANSACTION;");
        }
      }

      $this->PDO->exec("COMMIT;");
      $this->PreparedInsertStatement->closeCursor();

      if ($this->db_analyzed == false)
      {
        $this->PDO->exec("ANALYZE;");
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
      $this->PDO->exec("UPDATE urls SET processed = 1, in_process = 0, http_code = " . $http_code . " WHERE distinct_hash = '".$hash."';");
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
    $Result = $this->PDO->query("SELECT id FROM urls WHERE processed = 0 OR in_process = 1 LIMIT 1;");

    $has_columns = $Result->fetchColumn();

    $Result->closeCursor();

    PHPCrawlerBenchmark::stop("checking_for_urls_in_cache");

    if ($has_columns != false)
    {
      return true;
    }
    else return false;
    } catch (PDOException $e){
       
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
      $this->PDO->exec("UPDATE urls SET in_process = 0;");
    } catch (PDOException $e){
       
    } catch (Exception $e){
      
    }
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
    try
    {
      $this->PDO = new PDO("mysql:dbname={$this->mysql_db};host={$this->mysql_host}", $this->mysql_username, $this->mysql_password);
    }
    catch (Exception $e)
    {
      throw new Exception("Error creating MySQL-cache-file, ".$e->getMessage().", try installing mysql3-extension for PHP.");
    }

    $this->PDO->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_SILENT);
    $this->PDO->setAttribute(PDO::ATTR_TIMEOUT, 3000);

    if ($create_tables == true)
    {
      // Create url-table (if not exists)
      $this->PDO->exec("CREATE TABLE IF NOT EXISTS urls (id integer PRIMARY KEY AUTO_INCREMENT,
                                                         in_process bool DEFAULT 0,
                                                         processed bool DEFAULT 0,
                                                         priority_level integer,
                                                         distinct_hash TEXT,
                                                         link_raw TEXT,
                                                         linkcode TEXT,
                                                         linktext TEXT,
                                                         refering_url TEXT,
                                                         url_rebuild TEXT,
                                                         is_redirect_url bool,
                                                         http_code integer,
                                                         url_link_depth integer);");

      // Create indexes (seems that indexes make the whole thingy slower)
      $this->PDO->exec("ALTER TABLE priority_level ADD INDEX (priority_level);");
      $this->PDO->exec("ALTER TABLE distinct_hash ADD INDEX (distinct_hash);");
      $this->PDO->exec("ALTER TABLE in_process ADD INDEX (in_process);");
      $this->PDO->exec("ALTER TABLE processed ADD INDEX (processed);");

      $this->PDO->exec("ANALYZE;");
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
      // Prepared statement for URL-inserts
      $this->PreparedInsertStatement = $this->PDO->prepare("INSERT IGNORE INTO urls (priority_level, distinct_hash, link_raw, linkcode, linktext, refering_url, url_rebuild, is_redirect_url, url_link_depth)
                                                            VALUES(:priority_level,
                                                                   :distinct_hash,
                                                                   :link_raw,
                                                                   :linkcode,
                                                                   :linktext,
                                                                   :refering_url,
                                                                   :url_rebuild,
                                                                   :is_redirect_url,
                                                                   :url_link_depth);");
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
