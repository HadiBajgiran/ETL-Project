USE master;
GO

DROP DATABASE IF EXISTS tp_etl;
GO

-- nom de classement spécifique pour s'assurer que les comparaisons de chaînes sont uniformes
CREATE DATABASE tp_etl COLLATE SQL_Latin1_General_CP1_CI_AS;
GO

USE tp_etl
GO

-- Créer la table dim_date
CREATE TABLE dim_date(
	date_id INT IDENTITY(1,1) CONSTRAINT PK_dim_date PRIMARY KEY
	, date_value DATETIME NOT NULL
	, month_value TINYINT NOT NULL
	, day_value TINYINT NOT NULL
	, year_value SMALLINT NOT NULL
	, fiscalyear SMALLINT NOT NULL
);

-- Créer la table Fact_sales_cube
CREATE TABLE Fact_sales_cube(
	/*sale_id int IDENTITY(1,1) CONSTRAINT PK_dim_date PRIMARY KEY 
	,*/ProductNumber VARCHAR(30) NOT NULL
	, SubCategory VARCHAR(50) 
	, Category VARCHAR(50) 
	, date_id INT NOT NULL CONSTRAINT FK_fact_sales_dim_date REFERENCES dim_date(date_id)
	, TotalQuantity SMALLINT 
	, TotalAmount MONEY 
	, OrderCount SMALLINT 

	);

USE tp_etl
GO

-- Créer Une fonction pour trouver le premier lundi d'avril d'une année donnée
CREATE or ALTER FUNCTION dbo.firstmonday(@date date) returns date
AS
 BEGIN
 DECLARE @year VARCHAR(10)=CONVERT(VARCHAR, year(@date))
 DECLARE @Dpart TINYINT
 DECLARE @result DATE
 SET @year = @year+'-04-01'
 SET @Dpart= Datepart(DW,@year)
 SET @result= Case When @Dpart = 2 Then @year When @Dpart < 2 Then DateAdd(dd,2-@Dpart,@year) Else DateAdd(dd,(7-Datepart(DW,@year))+2,@year) End
 RETURN @result;
 END
GO
-- Tester que la fonction Rouler correctement
SELECT dbo.firstmonday('20130304')

-- Remplir la table dim_date avec les dates entre 20100101 et 20150101
INSERT INTO dim_date
	SELECT DISTINCT 
	 TheDate         = CONVERT(date, dat.Datet),
	 TheMonth        = DATEPART(MONTH,     dat.Datet),
	 TheDayvalue     = DATEPART(WEEKDAY,       dat.Datet),
	 TheYear         = DATEPART(YEAR,      dat.Datet),
	 fiscalyear      = Case When dbo.firstmonday(CONVERT(date, dat.Datet)) > CONVERT(date, dat.Datet) Then (YEAR(dat.Datet)-1) Else YEAR(dat.Datet) End
	 FROM (
		 SELECT  TOP (DATEDIFF(DAY, '20100101', '20150101') + 1)
        Datet = DATEADD(DAY, ROW_NUMBER() OVER(ORDER BY a.object_id) - 1, '20100101')
		FROM    sys.all_objects a ) as dat
	 ORDER BY TheDate 
GO

 -- Afficher le contenu de la table dim_date
SELECT *
FROM dim_date


-- Remplir la table Fact_sales_cube 
GO
CREATE or ALTER PROCEDURE import_Fact_sales_cube (@year smallint, @mois tinyint, @aFlushData bit)
As
-- Si @aFlushData=1, il faut effacer les données avec le mois/année 
IF @aFlushData=1
 BEGIN
   DELETE FROM Fact_sales_cube
   WHERE  date_id in (select de.date_id
						from Fact_sales_cube as fc
						inner join dim_date as de
						on de.date_id=fc.date_id 
						where year_value=@year and month_value=@mois)
   PRINT('Nous avons effacé les données avec le mois/année=' + CAST(@year AS CHAR(4))+'/'+ CAST(@mois AS CHAR(2)))
 END

 -- Si @aFlushData=0, Et si les données de l'année et du mois sont disponibles dans la table Fact_sales_cube, cela donne une erreur
 ELSE IF @aFlushData=0 and @year in (SELECT dd.year_value
										FROM Fact_sales_cube as fs
										inner join dim_date as dd 
											ON fs.date_id=dd.date_id) and @mois in (SELECT de.month_value
																						FROM Fact_sales_cube as fc
																						inner join dim_date as de
																						ON de.date_id=fc.date_id)
      
	  BEGIN
	  DECLARE
      @ErrorMessage  NVARCHAR(500)='ERREUR: Les données avec le mois/année=' + CAST(@year AS CHAR(4))+'/'+ CAST(@mois AS CHAR(2))+'sont deja dans la table'
        BEGIN TRY
         RAISERROR(@errorMessage, 11, 1);
         END TRY
       BEGIN CATCH
        SELECT
        @ErrorMessage = ERROR_MESSAGE();
	  RAISERROR(@ErrorMessage, 11, 1);
     END CATCH;	
	  END
 -- Si @aFlushData=0, Et si les données de l'année et du mois ne sont pas disponibles dans la table Fact_sales_cube, cela remplit la table
     ELSE 
        BEGIN TRY
		  INSERT INTO Fact_sales_cube(ProductNumber, SubCategory, Category, date_id, TotalQuantity, TotalAmount, OrderCount)
			SELECT hh.ProductNumber, hh.SubCategory , hh.Category, hh.date_id, Coalesce(sum(hh.OrderQty),0), Coalesce(sum(hh.LineTotal),0),  Coalesce(count(hh.SalesOrderID),0)
			 FROM (Select pp.ProductNumber, Coalesce(psc.[Name],'N/A') as SubCategory , Coalesce(ppc.[Name],'N/A') as Category, dimdat.date_id, sso.OrderQty, sso.LineTotal, sso.SalesOrderID
			 FROM dim_date as dimdat
		  	 Cross join AdventureWorks2019.Production.Product as pp
			 left join AdventureWorks2019.Production.ProductSubcategory as psc
			 ON pp.ProductSubcategoryID=psc.ProductSubcategoryID
			 left join AdventureWorks2019.Production.ProductCategory as ppc
			 ON psc.ProductCategoryID=ppc.ProductCategoryID
			 left join AdventureWorks2019.Sales.SalesOrderDetail as sso
			 ON sso.ProductID=pp.ProductID
		     WHERE dimdat.year_value=@year and dimdat.month_value=@mois) as hh
		     GROUP by hh.ProductNumber, hh.SubCategory, hh.Category, hh.date_id
		     ORDER by hh.date_id, hh.ProductNumber  
	    END TRY
 -- Si une erreur se produit lors du remplissage de la table, il génère une erreur et renvoie la table à son état précédent
         BEGIN CATCH   
			PRINT('')
		    PRINT('ErrorNumber = ' + CAST(ERROR_NUMBER() AS CHAR(10)))
			PRINT('ErrorSeverity = ' + CAST(ERROR_SEVERITY() AS CHAR(10)))
			PRINT('ErrorLine = ' + CAST(ERROR_LINE() AS CHAR(10)))
			PRINT('ErrorMessage = ' + CAST(ERROR_MESSAGE() AS CHAR(400)))
			IF @@TRANCOUNT > 0	
			 ROLLBACK TRANSACTION;
		END CATCH
	  IF @@TRANCOUNT > 0
	  COMMIT TRANSACTION;
  


--Exécuter la procédure avec l'année et le mois donnés
EXEC import_Fact_sales_cube @year=2012, @mois=3, @aFlushData=0

-- Afficher le contenu de la table Fact_sales_cube
SELECT *
FROM Fact_sales_cube

