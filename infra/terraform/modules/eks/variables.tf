variable "cluster_name" { type = string }
variable "cluster_version" {
  type    = string
  default = "1.32"
}
variable "vpc_id" { type = string }
variable "subnet_ids" { type = list(string) }
variable "node_count" {
  type    = number
  default = 3
}
