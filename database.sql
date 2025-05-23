CREATE TABLE IF NOT EXISTS `datasets`
(
	`id` INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    `name` VARCHAR(255) NOT NULL
)

CREATE TABLE IF NOT EXISTS `locations`
(
	`id` INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    `dataset_id` INT NOT NULL,
    `name` VARCHAR(255) NOT NULL,
    CONSTRAINT `fk_dataset_locations` FOREIGN KEY (`dataset_id`) REFERENCES `datasets`(`id`)
    	ON DELETE CASCADE ON UPDATE CASCADE
)

CREATE TABLE IF NOT EXISTS `timeseries`
(
    `id` INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    `dataset_id` INT NOT NULL,
    `name` VARCHAR(255) NOT NULL,
    FOREIGN KEY (`dataset_id`) REFERENCES `datasets`(`id`) ON DELETE CASCADE
)

CREATE TABLE  IF NOT EXISTS `timeseries_locations`
(
    `timeseries_id` INT NOT NULL,
    `location_id` INT NOT NULL,
    PRIMARY KEY (`timeseries_id`, `location_id`),
    FOREIGN KEY (`timeseries_id`) REFERENCES `timeseries`(`id`) ON DELETE CASCADE,
    FOREIGN KEY (`location_id`) REFERENCES `locations`(`id`) ON DELETE CASCADE
)

CREATE TABLE IF NOT EXISTS `predictions`
(
    `id` INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    `timeseries_id` INT NOT NULL,
    `name` VARCHAR(255) NOT NULL,
    `property_name` VARCHAR(255) NOT NULL,
    `train_date_from` DATE NOT NULL,
    `train_date_to` DATE NOT NULL,
    `forecast_date_from` DATE NOT NULL,
    `forecast_date_to` DATE NOT NULL,
    `forecast_path` VARCHAR(255) NOT NULL,
    `created_time` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (`timeseries_id`) REFERENCES `timeseries`(`id`) ON DELETE CASCADE
)