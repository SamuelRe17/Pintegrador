import com.github.tototoshi.csv._
import doobie._
import doobie.implicits._
import doobie.util.update.Update0
import cats._
import cats.effect._
import cats.effect.unsafe.implicits.global
import java.io.File
import cats.implicits._
import org.nspl._
import org.nspl.awtrenderer._
import org.nspl.data.HistogramData


implicit object CustomFormat extends DefaultCSVFormat {
  override val delimiter: Char = ';'
}
case class Tournament(matches_tournament_id: String, countryWinnerId: Int, tournaments_tournament_name: String, tournaments_year: Int, tournaments_count_teams: Int)

case class HostCountry(idHostCountry: Int, countryId: Int, tournamentId: String)

case class Team(teamid: String, nameCountryId: Int, region_name: String, mens_team: Int, womens_team: Int)

object Pintegrador {

  def main(args: Array[String]): Unit = {
    println("Conectando")

    val msql = Transactor.fromDriverManager[IO](
      driver = "com.mysql.cj.jdbc.Driver",
      url = "jdbc:mysql://localhost:3306/futbol",
      user = "root",
      pass = "samuel310309",
    )


    createCountryTable.transact(msql).unsafeRunSync()
    createTournamentsTable.transact(msql).unsafeRunSync()
    createHostCountryTable.transact(msql).unsafeRunSync()
    createPlayersTable.transact(msql).unsafeRunSync()
    createTeamsTable.transact(msql).unsafeRunSync()
    createSquadsTable.transact(msql).unsafeRunSync()
    createStadiumsTable.transact(msql).unsafeRunSync()
    createMatchesTable.transact(msql).unsafeRunSync()
    createGoalsTable.transact(msql).unsafeRunSync()

    println("Tablas creadas exitosamente.")

    val path2DataFile: String = "C://Users//samuc//OneDrive//Escritorio//shit//datasets//PartidosyGoles (1).csv"
    val reader = CSVReader.open(new File(path2DataFile))
    val contentFile: List[Map[String, String]] = reader.allWithHeaders()
    reader.close()

    val path2DataFile2: String = "C://Users//samuc//OneDrive//Escritorio//shit//datasets//AlineacionesXTorneofinal.csv"
    val reader2 = CSVReader.open(new File(path2DataFile2))
    val contentFile2: List[Map[String, String]] = reader2.allWithHeaders()
    reader2.close()

    exportFunc(contentFile, msql)
    exportFunc2(contentFile2, msql)
  }

  def createTournamentsTable: ConnectionIO[Int] =
    sql"""
      CREATE TABLE IF NOT EXISTS tournaments (
        matches_tournament_id VARCHAR(7) PRIMARY KEY,
        countryWinnerId INT,
        tournaments_tournament_name VARCHAR(50),
        tournaments_year INT,
        tournaments_count_teams INT,
        FOREIGN KEY (countryWinnerId) REFERENCES country(countryId)
      )
    """.update.run

  def createCountryTable: ConnectionIO[Int] =
    sql"""
      CREATE TABLE IF NOT EXISTS country (
        countryId INT PRIMARY KEY,
        countryName VARCHAR(25)
      )
    """.update.run

  def createHostCountryTable: ConnectionIO[Int] =
    sql"""
      CREATE TABLE IF NOT EXISTS host_country (
        idHostCountry INT PRIMARY KEY,
        countryId INT,
        tournamentId VARCHAR(7),
        FOREIGN KEY (countryId) REFERENCES country(countryId),
        FOREIGN KEY (tournamentId) REFERENCES tournaments(matches_tournament_id)
      )
    """.update.run

  def createPlayersTable: ConnectionIO[Int] =
    sql"""
      CREATE TABLE IF NOT EXISTS players (
        player_id VARCHAR(50) PRIMARY KEY,
        family_name VARCHAR(30),
        given_name VARCHAR(25),
        birth_date VARCHAR(25),
        female INT,
        goalKeeper INT,
        defender INT,
        midfielder INT,
        forward INT
      )
    """.update.run

  def createSquadsTable: ConnectionIO[Int] =
    sql"""
      CREATE TABLE IF NOT EXISTS squads (
        squads_team_id VARCHAR(50),
        squads_tournament_id VARCHAR(50),
        squads_player_id VARCHAR(8),
        squads_shirt_number INT,
        squads_position_name VARCHAR(50),
        PRIMARY KEY (squads_player_id),
        FOREIGN KEY (squads_team_id) REFERENCES teams (teamid),
        FOREIGN KEY (squads_tournament_id) REFERENCES tournaments (matches_tournament_id)
      )
    """.update.run

  def createStadiumsTable: ConnectionIO[Int] =
    sql"""
      CREATE TABLE IF NOT EXISTS stadiums (
        stadium_id VARCHAR(50) PRIMARY KEY,
        stadium_name VARCHAR(255),
        city_name VARCHAR(255),
        countryid INT,
        stadium_capacity INT,
        FOREIGN KEY (countryid) REFERENCES country(countryId)
      )
    """.update.run

  def createMatchesTable: ConnectionIO[Int] =
    sql"""
      CREATE TABLE IF NOT EXISTS matches (
        matches_tournament_id VARCHAR(50),
        home_team_name VARCHAR(255),
        away_team_name VARCHAR(255),
        matches_match_id VARCHAR(50) PRIMARY KEY,
        matches_away_team_id VARCHAR(50),
        matches_home_team_id VARCHAR(50),
        matches_stadium_id VARCHAR(50),
        matches_match_date VARCHAR(50),
        matches_match_time VARCHAR(50),
        matches_stage_name VARCHAR(50),
        matches_home_team_score INT,
        matches_away_team_score INT,
        matches_extra_time INT,
        matches_penalty_shootout INT,
        matches_home_team_score_penalties INT,
        matches_away_team_score_penalties INT,
        matches_result VARCHAR(50),
        FOREIGN KEY (matches_tournament_id) REFERENCES tournaments (matches_tournament_id),
        FOREIGN KEY (matches_away_team_id) REFERENCES squads (squads_team_id),
        FOREIGN KEY (matches_home_team_id) REFERENCES squads (squads_team_id),
        FOREIGN KEY (matches_stadium_id) REFERENCES stadiums (stadium_id)
      )
    """.update.run

  def createTeamsTable: ConnectionIO[Int] =
    sql"""
      CREATE TABLE IF NOT EXISTS teams (
        teamid VARCHAR(4) PRIMARY KEY,
        nameCountryId INT,
        region_name VARCHAR(255),
        mens_team INT,
        womens_team INT,
        FOREIGN KEY (nameCountryId) REFERENCES country (countryId)
      )
    """.update.run

  def createGoalsTable: ConnectionIO[Int] =
    sql"""
      CREATE TABLE IF NOT EXISTS goals (
        goals_goal_id VARCHAR(50) PRIMARY KEY,
        goals_team_id VARCHAR(50),
        goals_player_id VARCHAR(50),
        goals_minute_label VARCHAR(50),
        goals_minute_regulation INT,
        goals_minute_stoppage INT,
        goals_match_period VARCHAR(50),
        goals_own_goal INT,
        goals_penalty INT,
        FOREIGN KEY (goals_player_id) REFERENCES players (player_id),
        FOREIGN KEY (goals_team_id) REFERENCES squads (squads_team_id)
      )
    """.update.run



  def exportFunc(contentFile: List[Map[String, String]], msql: Transactor[IO]): Unit = {
    //dataForStadium(contentFile).foreach(update => update.run.transact(msql).unsafeRunSync())
    //generateData2MatchesTable(contentFile).foreach(update => update.run.transact(msql).unsafeRunSync())
    //generateDataTournament(contentFile)
    //generarDataCountry(contentFile)
    //generateHostCountryTable(contentFile)
    //generateDataTeamTable(contentFile)
    //generateDataGoalsTable(contentFile)
    val wc = "WC - 1930"

    println("Consulta sobre torneos:")
    val torneos = listAllTournaments().transact(msql).unsafeRunSync()
    torneos.foreach(println)

    println("\nConsulta sobre países anfitriones:")
    val hc = listAllHostCountries().transact(msql).unsafeRunSync()
    hc.foreach(println)

    println("\nConsulta sobre equipos:")
    val teams = listAllTeams().transact(msql).unsafeRunSync()
    teams.foreach(println)

    println("\nConsulta sobre ganadores por torneo:")
    val ganador = listWinnersByTournament().transact(msql).unsafeRunSync()
    ganador.foreach(println)

    println("\nConsulta sobre los mejores goleadores:")
    val top = listTopGoalScorers().transact(msql).unsafeRunSync()
    top.foreach(println)

    println("\nConsulta sobre equipos locales:")
    val home = listHomeTeams().transact(msql).unsafeRunSync()
    home.foreach(println)

    println("\nConsulta sobre equipos visitantes:")
    val away = listAwayTeams().transact(msql).unsafeRunSync()
    away.foreach(println)

    println("\nConsulta sobre estadios y capacidades:")
    val stadium = listStadiumsAndCapacities().transact(msql).unsafeRunSync()
    stadium.foreach(println)

    println("\nConsulta sobre partidos con resultados de penales:")
    val penales = listMatchesWithPenalties().transact(msql).unsafeRunSync()
    penales.foreach(println)

    println(s"\nConsulta sobre países participantes en el torneo $wc:")
    val Tour = listCountriesByTournament(wc).transact(msql).unsafeRunSync()
    Tour.foreach(println)

    minGoalMinute(msql)
    avgGoalMinute(msql)
    maxGoalMinute(msql)
    imagenes(contentFile)




    println("Datos subidos")

  }

  def exportFunc2(contentFile2: List[Map[String, String]], msql: Transactor[IO]): Unit = {

    //generateDataPlayerTable(contentFile2).foreach(update => update.run.transact(msql).unsafeRunSync())
    //generateData2squadTable(contentFile2).foreach(update => update.run.transact(msql).unsafeRunSync())
    imagenes2(contentFile2)

    println("Datos subidos")
  }

  def generarDataCountry(data: List[Map[String, String]]) =
    val insertFormat = s"INSERT INTO country(countryId, countryName) VALUES (%s, '%f');"
    val country = data.distinctBy(_("id_away_team_name"))
      .map(row => (
        row("id_away_team_name").toInt,
        row("away_team_name")
      ))
      .distinct
      .sorted
      .map(x => insertFormat.format(x._1, x._2))

    country.foreach(println)

  def generateDataTournament(data: List[Map[String, String]]) = {
    val insertFormat = "INSERT INTO tournaments(matches_tournament_id, countryWinnerId, tournaments_tournament_name, tournaments_year, tournaments_count_teams) VALUES('%s', %d, '%s', %d, %d);"
    val tournament = data.distinctBy(_("matches_tournament_id"))
      .map(row => (
        row("matches_tournament_id"),
        row("id_away_team_name").toInt,
        row("tournaments_tournament_name").replaceAll("'", ""),
        row("tournaments_year").toInt,
        row("tournaments_count_teams").toInt
      ))
      .distinct
      .sorted
      .map(t => insertFormat.format(t._1, t._2, t._3, t._4, t._5))

    tournament.foreach(println)
  }

  def dataForStadium(data: List[Map[String, String]]): List[Update0] =
    data
      .distinctBy(_("matches_stadium_id"))
      .map(row =>
        (
          row("matches_stadium_id"),
          row("stadiums_stadium_name").replaceAll("'", ""),
          row("stadiums_city_name").replaceAll("'", ""),
          row("id_away_team_name").toInt,
          row("stadiums_stadium_capacity").toInt
        )
      )
      .map { sta =>
        sql"""INSERT INTO stadiums(stadium_id, stadium_name, city_name, countryid, stadium_capacity)
              VALUES (${sta._1}, ${sta._2}, ${sta._3}, ${sta._4}, ${sta._5})
              LIMIT 0, 5000;""".update
      }


  def generateHostCountryTable(data: List[Map[String, String]]) = {
    val insertFormat = "INSERT INTO host_country(idHostCountry, countryId, tournamentId) VALUES (%d, %d, '%s');"
    val hostc = data.distinctBy(_("id_tournaments_host_country"))

      .map(row => (
        row("id_tournaments_host_country").toInt,
        row("id_away_team_name").toInt,
        row("matches_tournament_id")
      ))
      .distinct
      .sorted
      .map(t => insertFormat.format(t._1, t._2, t._3))

    hostc.foreach(println)
  }

  def generateDataTeamTable(data: List[Map[String, String]]) = {
    val insertFormat = "INSERT INTO teams(teamid, nameCountryId, region_name, mens_team, womens_team) VALUES ('%s', %d, '%s', %d, %d);"
    val team = data.distinctBy(_("matches_away_team_id"))
      .map(row => (
        row("matches_away_team_id"),
        row("id_away_team_name").toInt,
        row("away_region_name"),
        row("away_mens_team").toInt,
        row("away_womens_team").toInt
      ))
      .distinct
      .sorted
      .map(t => insertFormat.format(t._1, t._2, t._3, t._4, t._5))

    team.foreach(println)
  }

  def generateDataPlayerTable(data: List[Map[String, String]]): List[Update0] =
    data
      .distinctBy(_("squads_player_id"))
      .map(row =>
        (
          row("squads_player_id"),
          row("players_family_name").replaceAll("'", ""),
          row("players_given_name").replaceAll("'", ""),
          row("players_birth_date"),
          row("players_female").toInt,
          row("players_goal_keeper").toInt,
          row("players_defender").toInt,
          row("players_midfielder").toInt,
          row("players_forward").toInt
        )
      )
      .map { p =>
        sql"""INSERT INTO players(player_id,family_name,given_name,birth_date,female,goalKeeper,defender,midfielder,forward)
                VALUES (${p._1}, ${p._2}, ${p._3}, ${p._4}, ${p._5}, ${p._6}, ${p._7}, ${p._8}, ${p._9})""".update
      }




  def generateData2squadTable(data: List[Map[String, String]]): List[Update0] =
    data
      .distinctBy(_("squads_team_id"))
      .map(row =>(
          row("squads_team_id"),
          row("squads_tournament_id"),
          row("squads_player_id"),
          row("squads_shirt_number").toInt,
          row("squads_position_name")
        )
      )
      .map { sq =>
        sql"""INSERT INTO squads(squads_team_id, squads_tournament_id, squads_player_id, squads_shirt_number, squads_position_name)
               VALUES(${sq._1}, ${sq._2}, ${sq._3}, ${sq._4}, ${sq._5})""".update
      }


  def generateData2MatchesTable(data: List[Map[String, String]]): List[Update0] = {
    data
      .distinctBy(_("matches_match_id"))
      .map(row =>
        (
          row("matches_tournament_id"),
          row("home_team_name"),
          row("away_team_name"),
          row("matches_match_id"),
          row("matches_away_team_id"),
          row("matches_home_team_id"),
          row("matches_stadium_id"),
          row("matches_match_date"),
          row("matches_match_time"),
          row("matches_stage_name"),
          row("matches_home_team_score"),
          row("matches_away_team_score"),
          row("matches_extra_time"),
          row("matches_penalty_shootout"),
          row("matches_home_team_score_penalties"),
          row("matches_away_team_score_penalties"),
          row("matches_result")
        )
      )
      .map { mat =>
        sql"""INSERT INTO matches(matches_tournament_id, home_team_name, away_team_name, matches_match_id, matches_away_team_id, matches_home_team_id, matches_stadium_id, matches_match_date, matches_match_time, matches_stage_name, matches_home_team_score, matches_away_team_score, matches_extra_time, matches_penalty_shootout, matches_home_team_score_penalties, matches_away_team_score_penalties, matches_result)
              VALUES (${mat._1}, ${mat._2}, ${mat._3}, ${mat._4}, ${mat._5}, ${mat._6}, ${mat._7}, ${mat._8}, ${mat._9}, ${mat._10}, ${mat._11}, ${mat._12}, ${mat._13}, ${mat._14}, ${mat._15}, ${mat._16}, ${mat._17});""".update
      }
  }

  def generateDataGoalsTable(data: List[Map[String, String]]) =

    val insertFormat = s"INSERT INTO goals(goals_goal_id,goals_team_id,goals_player_id,goals_minute_label,goals_minute_regulation,goals_minute_stoppage,goals_match_period,goals_own_goal,goals_penalty)" +
      s"VALUES('%s','%s', '%s', '%s', %d, %d, '%s', %d, %d);"

    val info = data
      .filterNot(_("goals_goal_id") == "NA")
      .map(row => (
        row("goals_goal_id"),
        row("goals_team_id"),
        row("goals_player_id"),
        row("goals_minute_label").replaceAll("'", ""),
        row("goals_minute_regulation").toInt,
        row("goals_minute_stoppage").toInt,
        row("goals_match_period"),
        row("goals_own_goal").toInt,
        row("goals_penalty").toInt
      ))
      .distinct
      .sorted
      .map(g => insertFormat.format(g._1,g._2, g._3, g._4, g._5, g._6,g._7,g._8,g._9))

    info.foreach(println)


  def listAllTournaments(): ConnectionIO[List[Tournament]] =
    sql"SELECT t.matches_tournament_id, t.countryWinnerId, t.tournaments_tournament_name, t.tournaments_year, t.tournaments_count_teams FROM tournaments t"
      .query[Tournament]
      .to[List]



  def listAllHostCountries(): ConnectionIO[List[HostCountry]] =
    sql"SELECT hc.idHostCountry, hc.countryId, hc.tournamentId FROM host_country hc"
      .query[HostCountry]
      .to[List]


  def listAllTeams(): ConnectionIO[List[Team]] =
    sql"SELECT tm.teamid, tm.nameCountryId, tm.region_name, tm.mens_team, tm.womens_team FROM teams tm"
      .query[Team]
      .to[List]

  def listWinnersByTournament(): ConnectionIO[List[(String, String)]] =
    sql"SELECT t.matches_tournament_id, c.countryName FROM tournaments t JOIN country c ON t.countryWinnerId = c.countryId"
      .query[(String, String)]
      .to[List]

  def listTopGoalScorers(): ConnectionIO[List[(String, String, Int)]] =
    sql"""
      SELECT p.family_name, p.given_name, COUNT(*) as totalGoals
      FROM goals g
      JOIN players p ON g.goals_player_id = p.player_id
      GROUP BY g.goals_player_id
      ORDER BY totalGoals DESC
      LIMIT 5
    """
      .query[(String, String, Int)]
      .to[List]

  def listHomeTeams(): ConnectionIO[List[String]] =
    sql"SELECT DISTINCT m.home_team_name FROM matches m"
      .query[String]
      .to[List]

  def listAwayTeams(): ConnectionIO[List[String]] =
    sql"SELECT DISTINCT m.away_team_name FROM matches m"
      .query[String]
      .to[List]

  def listStadiumsAndCapacities(): ConnectionIO[List[(String, String, Int)]] =
    sql"SELECT s.stadium_name, s.city_name, s.stadium_capacity FROM stadiums s"
      .query[(String, String, Int)]
      .to[List]

  def listMatchesWithPenalties(): ConnectionIO[List[(String, String, String, Int, Int)]] =
    sql"SELECT m.matches_tournament_id, m.home_team_name, m.away_team_name, m.matches_home_team_score_penalties, m.matches_away_team_score_penalties FROM matches m WHERE m.matches_home_team_score_penalties IS NOT NULL OR m.matches_away_team_score_penalties IS NOT NULL"
      .query[(String, String, String, Int, Int)]
      .to[List]

  def listCountriesByTournament(tournamentId: String): ConnectionIO[List[String]] =
    sql"SELECT DISTINCT t.nameCountryId FROM teams t JOIN squads s ON t.teamid = s.squads_team_id WHERE s.squads_tournament_id = $tournamentId "
      .query[String]
      .to[List]

  def listGoalMinutes(): ConnectionIO[List[Int]] =
    sql"SELECT goals_minute_regulation FROM goals"
      .query[Int]
      .to[List]


  def minGoalMinute(msql: Transactor[IO]): Unit = {
    val goalsMinutes = listGoalMinutes().transact(msql).unsafeRunSync()
    val minMinute = goalsMinutes.min
    println(s"Mínimo Minuto de Gol: $minMinute")

  }

  def avgGoalMinute(msql: Transactor[IO]): Unit = {
    val goalsMinutes = listGoalMinutes().transact(msql).unsafeRunSync()
    val avgMinute = goalsMinutes.sum.toDouble / goalsMinutes.size.toDouble
    println(f"Promedio Minuto de Gol: $avgMinute%.2f")
  }

  def maxGoalMinute(msql: Transactor[IO]): Unit = {
    val maxMinute = listGoalMinutes().transact(msql).unsafeRunSync().max
    println(s"Máximo Minuto de Gol: $maxMinute")
  }

  def imagenes(data: List[Map[String, String]]): Unit = {


    val GolesMLocales: List[Double] = data
      .filter(row => row.contains("matches_home_team_score"))
      .map(row => row("matches_home_team_score").toDouble)

    val histHomeTeamScores = xyplot(HistogramData(GolesMLocales, 20) -> bar())(
      par
        .xlab("Goles Por Locales")
        .ylab("freq.")
        .main("Histograma home team score")
    )

    pngToFile(new File("D:\\program\\Pintegrador\\GolesLocales.png"),
      histHomeTeamScores.build, width = 1000)

    val golesMarcados: List[Double] = data
      .filter(row => row.contains("matches_home_team_score") && row.contains("matches_away_team_score"))
      .flatMap(row => List(row("matches_home_team_score").toDouble, row("matches_away_team_score").toDouble))

    val histGoles = xyplot(HistogramData(golesMarcados, 20) -> bar())(
      par
        .xlab("Cantidad Total de Goles en un Partido")
        .ylab("Frecuencia")
        .main("Histograma de Cantidad Total de Goles en un Partido")
    )

    pngToFile(new File("D:\\program\\Pintegrador\\GolesHistograma.png"), histGoles.build, width = 1000)

    val listGoals: List[Double] = data
      .filter(row => row.contains("goals_minute_regulation") && row("goals_minute_regulation").nonEmpty)
      .map(row => row("goals_minute_regulation").toDouble)


    val golesVsMinutos = xyplot(listGoals -> point())(
      par
        .xlab("Minuto de Gol")
        .ylab("Cantidad de Goles")
        .main("Relación entre Minuto de Gol y Cantidad de Goles")
    )

    pngToFile(new File("D:\\program\\Pintegrador\\goles_vs_minutos.png"),
      golesVsMinutos.build, width = 1000)


    val VisitanteMGoles: List[Double] = data
      .filter(row => row.contains("matches_away_team_score"))
      .map(row => row("matches_away_team_score").toDouble)

    val histAwayTeamScores = xyplot(HistogramData(VisitanteMGoles, 20) -> line())(
      par
        .xlab("away team score")
        .ylab("freq.")
        .main("Histograma away team score")
    )

    pngToFile(new File("D:\\program\\Pintegrador\\GolesVisitantes.png"),
      histAwayTeamScores.build, width = 1000)




    val listStadiumCapacities: List[Double] = data
      .filter(row => row.contains("stadiums_stadium_capacity"))
      .map(row => row("stadiums_stadium_capacity").toDouble)

    val stadiumCapacityPlot = xyplot(listStadiumCapacities -> point())(
      par
        .xlab("Capacidad del Estadio")
        .ylab("Frecuencia")
        .main("Relación entre Capacidad del Estadio y Frecuencia")
    )

    pngToFile(new File("D:\\program\\Pintegrador\\capacidadestadios.png"),
      stadiumCapacityPlot.build, width = 1000)





  }
  def imagenes2(data: List[Map[String, String]]): Unit = {
    val listNroShirt: List[Double] = data
      .filter(row => row.contains("squads_shirt_number"))
      .map(row => row("squads_shirt_number").toDouble)

    val histMidfielderShirtNumber = xyplot(HistogramData(listNroShirt, 20) -> line())(
      par
        .xlab("Shirt number")
        .ylab("freq.")
        .main("Numero de camiseta")
    )

    pngToFile(new File("D:\\program\\Pintegrador\\frecuencia_numeros_camiseta.png"),
      histMidfielderShirtNumber.build, width = 1000)

  }



}
