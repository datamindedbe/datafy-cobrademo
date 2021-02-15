locals {
  service_account_name = "cobrademo"
}

resource "kubernetes_service_account" "default" {
  metadata {
    name = local.service_account_name
    namespace = var.env_name
    annotations = {
      "eks.amazonaws.com/role-arn" : aws_iam_role.default.arn
    }
  }
  automount_service_account_token = true
}
resource "aws_iam_role" "default" {
  name               = "cobrademo-${var.env_name}"
  path               = "/datafy-dp-${var.env_name}/"
  assume_role_policy = data.aws_iam_policy_document.default.json
}

data "aws_iam_policy_document" "default" {
  statement {
    actions = [
      "sts:AssumeRoleWithWebIdentity"]
    effect = "Allow"

    condition {
      test = "StringEquals"
      variable = "${replace(var.aws_iam_openid_connect_provider_url, "https://", "")}:sub"
      values = [
        "system:serviceaccount:${var.env_name}:${local.service_account_name}"]
    }

    principals {
      identifiers = [
        var.aws_iam_openid_connect_provider_arn]
      type = "Federated"
    }
  }
}
//
//resource "aws_iam_policy" "s3_permissions_policy" {
//  policy =data.aws_iam_policy_document.s3_premissions.json
//  name = "datafy-cobrademo-${var.env_name}-s3-permissions-policy"
//}

data "aws_iam_policy_document" "s3_glue_permissions" {
  statement {
    actions = [
      "s3:*",
    ]
    resources = [
      "arn:aws:s3:::datafy-training",
      "arn:aws:s3:::datafy-training/cobra/*"
    ]
  }

  statement {
    actions = [
      "glue:*",
    ]
    resources = [
      "*"
    ]
  }
}

resource "aws_iam_role_policy" "s3_glue_permissions" {
  name   = "s3-glue-permissions"
  role   = aws_iam_role.default.id
  policy = data.aws_iam_policy_document.s3_glue_permissions.json
}
//resource "aws_iam_role_policy_attachment" "default" {
//  policy_arn = aws_iam_policy.s3_permissions_policy.arn
//  role = aws_iam_role.default.name
//}